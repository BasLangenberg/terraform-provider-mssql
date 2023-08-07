package mssql

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccSchema_Local_BasicImport(t *testing.T) {
  resource.Test(t, resource.TestCase{
    PreCheck:          func() { testAccPreCheck(t) },
    IsUnitTest:        runLocalAccTests,
    ProviderFactories: testAccProviders,
    CheckDestroy:      func(state *terraform.State) error { return testAccCheckSchemaDestroy(state) },
    Steps: []resource.TestStep{
      {
        Config: testAccCheckSchema(t, "test_import", "testschema", map[string]interface{}{}),
        Check: resource.ComposeTestCheckFunc(
          testAccCheckSchemaExists("mssql_schema.test_import"),
        ),
      },
      {
        ResourceName:      "mssql_schema.test_import",
        ImportState:       true,
        ImportStateVerify: true,
        ImportStateIdFunc: testAccImportStateId("mssql_schema.test_import", false),
      },
    },
  })
}
