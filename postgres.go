package iris_pg

import (
	"fmt"
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/kataras/iris/v12"
	"github.com/pelletier/go-toml"
	"log"
	"regexp"
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

func (p *PostgresInstance) CreateIndexes(schemas iris.Map, indexes map[string][]string) {
	db := p.db
	pattern := regexp.MustCompile(`[^\w\-]`)
	keywords := []string{"btree", "hash", "gist", "spgist", "gin", "brin", "asc", "desc", "nulls", "first", "last"}
	for key, values := range indexes {
		if _, ok := schemas[key]; ok {
			name := PostgresTableName[key]
			for _, value := range values {
				expr := StripKeywords(strings.ToLower(value), keywords)
				columns := pattern.ReplaceAllString(expr, "")
				idx := NormalizeName(name + "_" + columns + "_idx")
				format := "CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s USING %s"
				query := fmt.Sprintf(format, idx, name, value)
				if _, err := db.Exec(query); err != nil {
					log.Println(query)
					log.Println(err)
				}
			}
		}
	}
}
