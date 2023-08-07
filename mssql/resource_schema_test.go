package mssql

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestSchema_Create(t *testing.T) {
  resource.Test(t, resource.TestCase{
    PreCheck:          func() { testAccPreCheck(t) },
    IsUnitTest:        runLocalAccTests,
    ProviderFactories: testAccProviders,
    CheckDestroy:      func(state *terraform.State) error { return testAccCheckSchemaDestroy(state) },
    Steps: []resource.TestStep{
      {
        Config: testAccCheckSchema(t, "test_create", "testschema", map[string]interface{}{}),
        Check: resource.ComposeTestCheckFunc(
          testAccCheckSchemaExists("mssql_schema.test_create"),
          resource.TestCheckResourceAttr("mssql_schema.test_create", "database", "master"),
          resource.TestCheckResourceAttr("mssql_schema.test_create", "schema_name", "testschema"),
          resource.TestCheckResourceAttr("mssql_schema.test_create", "server.0.login.0.username", os.Getenv("MSSQL_USERNAME")),
          resource.TestCheckResourceAttr("mssql_schema.test_create", "server.0.login.0.password", os.Getenv("MSSQL_PASSWORD")),
          resource.TestCheckResourceAttrSet("mssql_schema.test_create", "schema_id"),
          resource.TestCheckResourceAttrSet("mssql_schema.test_create", "principal_id"),
        ),
      },
    },
  })
}

func testAccCheckSchema(t *testing.T, name string, schemaName string, data map[string]interface{}) string {
  text := `resource "mssql_schema" "{{ .name }}" {
             server {
               host = "localhost"
         login {}
             }
       {{ with .database }}database = "{{ . }}"{{ end }}
             schema_name = "{{ .schema_name }}"
           }`

  data["name"] = name

  data["schema_name"] = schemaName
  data["host"] = "localhost"

  res, err := templateToString(schemaName, text, data)
  if err != nil {
    t.Fatalf("%s", err)
  }
  return res
}

func testAccCheckSchemaDestroy(state *terraform.State) error {
  for _, rs := range state.RootModule().Resources {
    if rs.Type != "mssql_schema" {
      continue
    }

    connector, err := getTestConnector(rs.Primary.Attributes)
    if err != nil {
      return err
    }

    database := rs.Primary.Attributes["database"]
    schemaName := rs.Primary.Attributes["schema_name"]
    schema, err := connector.GetSchema(database, schemaName)
    if schema != nil {
      return fmt.Errorf("schema still exists")
    }
    if err != nil {
      return fmt.Errorf("expected no error, got %s", err)
    }
  }
  return nil
}

func testAccCheckSchemaExists(resource string) resource.TestCheckFunc {
  return func(state *terraform.State) error {
    rs, ok := state.RootModule().Resources[resource]
    if !ok {
      return fmt.Errorf("not found: %s", resource)
    }
    if rs.Type != "mssql_schema" {
      return fmt.Errorf("expected resource of type %s, got %s", "mssql_schema", rs.Type)
    }
    if rs.Primary.ID == "" {
      return fmt.Errorf("no record ID is set")
    }
    connector, err := getTestConnector(rs.Primary.Attributes)
    if err != nil {
      return err
    }
    database := rs.Primary.Attributes["database"]
    schemaName := rs.Primary.Attributes["schema_name"]
    schema, err := connector.GetSchema(database, schemaName)
    if err != nil {
      return fmt.Errorf("error: %s", err)
    }
    if schema.SchemaName != schemaName {
      return fmt.Errorf("expected to be schema %s, got %s", schemaName, schema.SchemaName)
    }
    return nil
  }
}
