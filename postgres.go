package iris_pg

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/kataras/iris/v12"
	"github.com/pelletier/go-toml"
	"log"
	"strings"
)

var (
	PostgresTableName = map[string]string{}
)

type PostgresInstance struct {
	db *pg.DB
}

func (pg *PostgresInstance) CreateSchema(cfg *toml.Tree, schemas iris.Map, citus iris.Map) {
	db := pg.db
	postgres := GetTree(cfg, "postgres")
	namespace := GetString(postgres, "namespace")
	enableCitus := GetBool(postgres, "enable-citus")
	pluralizeTableName := GetBool(postgres, "pluralize-table-name")
	tableOptions := &orm.CreateTableOptions{
		IfNotExists:   true,
		FKConstraints: true,
	}

	for key, schema := range schemas {
		name := strings.ToLower(key)
		if pluralizeTableName {
			name = Plural(name)
		}
		if namespace != "" {
			name = namespace + "_" + name
		}
		PostgresTableName[key] = name
		if err := db.CreateTable(schema, tableOptions); err != nil {
			log.Println(key, schema)
			log.Fatalln(err)
		}
		if pk, ok := citus[key]; ok && enableCitus {
			query := "SELECT create_distributed_table(?, ?)"
			if _, err := db.Exec(query, name, pk); err != nil {
				log.Println(query)
				log.Println(err)
			}
		}
	}

}
