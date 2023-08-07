package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/betr-io/terraform-provider-mssql/mssql/model"
)

func (c *Connector) GetSchema(ctx context.Context, database, schemaname string) (*model.Schema, error) {
  cmd := "SELECT * FROM sys.schemas where name = @schema"

  var (
    schema  model.Schema
  )

  err := c.
    setDatabase(&database).
    QueryRowContext(ctx, cmd,
      func(r *sql.Row) error {
        return r.Scan(&schema.SchemaName, &schema.SchemaID, &schema.PrincipalID)
      },
      sql.Named("schema", schemaname),
    )
  if err != nil {
    if err == sql.ErrNoRows {
      return nil, nil
    }
    return nil, err
  }
  return &schema, nil
}

func (c *Connector) CreateSchema(ctx context.Context, database string, schema *model.Schema) error {
  cmd := fmt.Sprintf("CREATE SCHEMA %s AUTHORIZATION [dbo]", schema.SchemaName)
  //cmd := "EXEC('CREATE SCHEMA ' + @schema)"

//  return c.
//    setDatabase(&database).
//    ExecContext(ctx, cmd,
//      sql.Named("schema", schema.SchemaName),
//    )
  return c.
    setDatabase(&database).
    ExecContext(ctx, cmd)
}

//
// Due to complex dynamic SQL requirements moving tables from old to new schemas
// It has been decided we will omit this functionality for now
// Info: https://www.geeksforgeeks.org/how-to-rename-sql-server-schema/
//

//func (c *Connector) UpdateSchema(ctx context.Context, database string, user *model.Schema) error {
//  cmd := `DECLARE @stmt nvarchar(max)
//          EXEC (@stmt)`
//  return c.
//    setDatabase(&database).
//    ExecContext(ctx, cmd,
//      sql.Named("database", database),
//      sql.Named("username", user.Username),
//      sql.Named("defaultSchema", user.DefaultSchema),
//      sql.Named("defaultLanguage", user.DefaultLanguage),
//      sql.Named("roles", strings.Join(user.Roles, ",")),
//    )
//}

func (c *Connector) DeleteSchema(ctx context.Context, database, schemaname string) error {
  cmd := fmt.Sprintf("DROP SCHEMA %s", schemaname)
  //cmd := "EXEC('DROP SCHEMA ' + @schema)"

  return c.
    setDatabase(&database).
    //ExecContext(ctx, cmd, sql.Named("schemaname", schemaname))
    ExecContext(ctx, cmd)
}

