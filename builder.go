package cassandraQB

import (
	"fmt"
	"github.com/gocql/gocql"
	"strings"
)

type Connection struct {
	Cluster *gocql.ClusterConfig
	Session *gocql.Session
}

type TableMetadata struct {
	Table      string
	Columns    map[string]struct{}
	Pk         map[string]struct{}
	Ck         map[string]struct{}
	Keyspace   string
	DependsOn  TableDependencies
	Connection *gocql.Session
}

type TableDependencies []TableDependency
type TableDependency func(map[string]interface{}, *gocql.Batch) bool

func FilterData(data map[string]interface{}, metaData TableMetadata) map[string]interface{} {
	values := make(map[string]interface{})
	for column, _ := range metaData.Columns {
		value, isset := data[column]
		switch isset {
		case true:
			values[column] = value
		}
	}
	return values
}

func GenerateEmptyInputs(count int) string {
	var inputs []string
	for i := 0; i < count; i++ {
		inputs = append(inputs, "?")
	}
	return strings.Join(inputs, ",")
}

func BindArgs(data map[string]interface{}) ([]interface{}, []string) {
	Args := []interface{}{}
	fields := make([]string, 0)
	for field, value := range data {
		Args = append(Args, value)
		fields = append(fields, field)
	}
	return Args, fields
}

func (metaData *TableMetadata) NewRecord(values map[string]interface{}, batch *gocql.Batch) error {
	err := CheckData(&values, *metaData)
	switch err != nil {
	case true:
		return err
	}

	Args, fields := BindArgs(values)
	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO " + metaData.Table + " (" + strings.Join(fields, ",") + ") VALUES (" + GenerateEmptyInputs(len(values)) + ")",
		Args:       Args,
		Idempotent: false,
	})
	return nil
}

func (metaData *TableMetadata) GetSelectStatement(conditions map[string]interface{}, selectedFields []string) (statement *gocql.Query, err error) {
	err = CheckData(&conditions, *metaData)
	switch err != nil {
	case true:
		return
	}

	session := metaData.Connection
	Args, fields := BindArgs(conditions)
	statement = bindValues(session.Query("SELECT "+strings.Join(selectedFields, ",")+" FROM "+metaData.Table+" WHERE "+GenerateWhereConditions(fields)), Args)

	return
}

func (metaData *TableMetadata) GetRecord(conditions map[string]interface{}, selectedFields []string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})

	statement, err := metaData.GetSelectStatement(conditions, selectedFields)
	switch err != nil {
	case true:
		return
	}

	err = statement.MapScan(data)

	return
}

func (metaData *TableMetadata) UpdateRecord(conditions map[string]interface{}, values map[string]interface{}, batch *gocql.Batch) error {
	err := CheckData(&conditions, *metaData)
	switch err != nil {
	case true:
		return err
	}

	Args, fields := BindArgs(values)
	whereArgs, whereFields := BindArgs(conditions)
	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt:       "UPDATE " + metaData.Table + " SET " + strings.Join(generateEmptyEquality(fields), ",") + " WHERE " + GenerateWhereConditions(whereFields),
		Args:       MergeArgs(Args, whereArgs),
		Idempotent: false,
	})

	return nil
}

func (metaData *TableMetadata) DeleteRecord(conditions map[string]interface{}, batch *gocql.Batch) error {
	err := CheckData(&conditions, *metaData)
	switch err != nil {
	case true:
		return err
	}

	args, fields := BindArgs(conditions)
	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt: "DELETE FROM " + metaData.Table + " WHERE " + GenerateWhereConditions(fields),
		Args: args,
	})

	return nil
}

func MergeArgs(args ...[]interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, arg := range args {
		for _, element := range arg {
			result = append(result, element)
		}
	}

	return result
}

func bindValues(statement *gocql.Query, args []interface{}) *gocql.Query {
	for _, value := range args {
		statement = statement.Bind(value)
	}
	return statement
}

func generateEmptyEquality(fields []string) []string {
	for index, value := range fields {
		fields[index] = value + " = ?"
	}
	return fields
}

func GenerateWhereConditions(fields []string) string {
	fields = generateEmptyEquality(fields)

	return strings.Join(fields, " AND ")
}

func CheckData(values *map[string]interface{}, metaData TableMetadata) error {
	data := FilterData(*values, metaData)
	switch len(data) == 0 {
	case true:
		return ErrorEmptyValidData
	}
	switch CheckPK(metaData, &data) {
	case false:
		return ErrorPrimaryKeyMissing
	}

	values = &data
	return nil
}

func AddDependencies(dependencies TableDependencies, values map[string]interface{}, statement *gocql.Batch) bool {
	isSuccessful := true
	channel := make(chan bool)
	taskNum := 0

	for _, dependency := range dependencies {
		taskNum++
		go addDependency(channel, dependency, values, statement)
	}

	for isSucceed := range channel {
		fmt.Println("succeed:", isSucceed)
		isSuccessful = isSuccessful && isSucceed
		taskNum--

		if taskNum == 0 {
			fmt.Println("getting out")
			break
		}
	}
	fmt.Println("finished")

	return isSuccessful
}

func addDependency(channel chan bool, dependency TableDependency, values map[string]interface{}, statement *gocql.Batch) {
	channel <- dependency(values, statement)
}

func CheckPK(metaData TableMetadata, data *map[string]interface{}) bool {
	for field := range metaData.Pk {
		switch _, isSet := (*data)[field]; isSet {
		case false:
			return false
		}
	}
	return true
}
