package Cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/zytell3301/uuid-generator"
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

func AddId(values *map[string]interface{}, idName interface{}, generator uuid_generator.Generator) (err error) {
	var id *uuid.UUID

	switch idName == nil {
	case true:
		id, err = generator.GenerateV4()
		switch err != nil {
		case true:
			return
		}
		break
	default:
		id = generator.GenerateV5(idName.(string))
	}

	_, isset := (*values)["id"]
	switch isset {
	case false:
		(*values)["id"] = id
	}

	return
}

func (metaData *TableMetadata) NewRecord(values map[string]interface{}, batch *gocql.Batch) bool {
	switch CheckData(&values, *metaData) {
	case false:
		return false
	}

	Args, fields := BindArgs(values)
	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO " + metaData.Table + " (" + strings.Join(fields, ",") + ") VALUES (" + GenerateEmptyInputs(len(values)) + ")",
		Args:       Args,
		Idempotent: false,
	})
	return true
}

func (metaData *TableMetadata) GetSelectStatement(conditions map[string]interface{}, selectedFields []string) (statement *gocql.Query) {
	switch CheckData(&conditions, *metaData) {
	case false:
		return
	}

	session := metaData.Connection
	Args, fields := BindArgs(conditions)
	statement = bindValues(session.Query("SELECT "+strings.Join(selectedFields, ",")+" FROM "+metaData.Table+" WHERE "+GenerateWhereConditions(fields)), Args)

	return
}

func (metaData *TableMetadata) GetRecord(conditions map[string]interface{}, selectedFields []string) (data map[string]interface{}) {
	data = make(map[string]interface{})

	statement := metaData.GetSelectStatement(conditions, selectedFields)
	switch statement == nil {
	case true:
		return
	}

	statement.MapScan(data)

	return
}

func (metaData *TableMetadata) UpdateRecord(conditions map[string]interface{}, values map[string]interface{}, batch *gocql.Batch) bool {
	switch CheckData(&conditions, *metaData) {
	case false:
		return false
	}

	Args, fields := BindArgs(values)
	whereArgs, whereFields := BindArgs(conditions)
	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt:       "UPDATE " + metaData.Table + " SET " + GenerateWhereConditions(fields) + " WHERE " + GenerateWhereConditions(whereFields),
		Args:       MergeArgs(Args, whereArgs),
		Idempotent: false,
	})

	return true
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

func GenerateWhereConditions(fields []string) string {
	for index, value := range fields {
		fields[index] = value + " = ?"
	}

	return strings.Join(fields, " AND ")
}

func CheckData(values *map[string]interface{}, metaData TableMetadata) bool {
	data := FilterData(*values, metaData)
	switch len(data) == 0 {
	case true:
		return false
	}
	switch CheckPK(metaData, &data) {
	case false:
		return false
	}

	values = &data
	return true
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
