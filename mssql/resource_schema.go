package mssql

import (
	"context"
	"strings"

	"github.com/betr-io/terraform-provider-mssql/mssql/model"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/pkg/errors"
)

func resourceSchema() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceSchemaCreate,
		ReadContext:   resourceSchemaRead,
		DeleteContext: resourceSchemaDelete,
		Importer: &schema.ResourceImporter{
			StateContext: resourceSchemaImport,
		},
		Schema: map[string]*schema.Schema{
			serverProp: {
				Type:     schema.TypeList,
				MaxItems: 1,
				Required: true,
				ForceNew: true,
				Elem: &schema.Resource{
					Schema: getServerSchema(serverProp),
				},
			},
			databaseProp: {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				Default:  "master",
			},
			schemaNameProp: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			schemaIdProp: {
				Type:     schema.TypeString,
				Computed: true,
//				ForceNew: true,
			},
			principalIdProp: {
				Type:     schema.TypeString,
				Computed: true,
//				ForceNew: true,
			},
		},
		Timeouts: &schema.ResourceTimeout{
			Default: defaultTimeout,
		},
	}
}

type SchemaConnector interface {
	CreateSchema(ctx context.Context, database string, schema *model.Schema) error
	GetSchema(ctx context.Context, database, schemaname string) (*model.Schema, error)
	DeleteSchema(ctx context.Context, database, schemaname string) error
}

func resourceSchemaCreate(ctx context.Context, data *schema.ResourceData, meta interface{}) diag.Diagnostics {
	logger := loggerFromMeta(meta, "schema", "create")

	database := data.Get(databaseProp).(string)
	schemaname := data.Get(schemaNameProp).(string)

	connector, err := getSchemaConnector(meta, data)
	if err != nil {
		return diag.FromErr(err)
	}

	schema := &model.Schema{
		SchemaName:        schemaname,
	}
	if err = connector.CreateSchema(ctx, database, schema); err != nil {
		return diag.FromErr(errors.Wrapf(err, "unable to create schema [%s] in database [%s]", schemaname, database))
	}

	data.SetId(getSchemaID(data))

	logger.Info().Msgf("created schema [%s]in database [%s]", schemaname, database)

	return resourceSchemaRead(ctx, data, meta)
}

func resourceSchemaRead(ctx context.Context, data *schema.ResourceData, meta interface{}) diag.Diagnostics {
	logger := loggerFromMeta(meta, "schema", "read")
	logger.Debug().Msgf("Read %s", data.Id())

	database := data.Get(databaseProp).(string)
	schemaname := data.Get(schemaNameProp).(string)

	connector, err := getSchemaConnector(meta, data)
	if err != nil {
		return diag.FromErr(err)
	}

	schema, err := connector.GetSchema(ctx, database, schemaname)
	if err != nil {
		return diag.FromErr(errors.Wrapf(err, "unable to read schema [%s] in database [%s]", schemaname, database))
	}
	if schema == nil {
		logger.Info().Msgf("No schema [%s] found in database [%s]", schemaname, database)
		data.SetId("")
	} else {
	  if err = data.Set(schemaNameProp, schema.SchemaName); err != nil {
	  	return diag.FromErr(err)
	  }
	  if err = data.Set(schemaIdProp, schema.SchemaID); err != nil {
	  	return diag.FromErr(err)
	  }
	  if err = data.Set(principalIdProp, schema.PrincipalID); err != nil {
	  	return diag.FromErr(err)
	  }
  }

	return nil
}

func resourceSchemaDelete(ctx context.Context, data *schema.ResourceData, meta interface{}) diag.Diagnostics {
	logger := loggerFromMeta(meta, "schema", "delete")
	logger.Debug().Msgf("Delete %s", data.Id())

	database := data.Get(databaseProp).(string)
	schemaname := data.Get(schemaNameProp).(string)

	connector, err := getSchemaConnector(meta, data)
	if err != nil {
		return diag.FromErr(err)
	}

	if err = connector.DeleteSchema(ctx, database, schemaname); err != nil {
		return diag.FromErr(errors.Wrapf(err, "unable to delete schema [%s].[%s]", database, schemaname))
	}

	logger.Info().Msgf("deleted user [%s].[%s]", database, schemaname)

	// d.SetId("") is automatically called assuming delete returns no errors, but it is added here for explicitness.
	data.SetId("")

	return nil
}

func resourceSchemaImport(ctx context.Context, data *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	logger := loggerFromMeta(meta, "schema", "import")
	logger.Debug().Msgf("Import %s", data.Id())

	server, u, err := serverFromId(data.Id())
	if err != nil {
		return nil, err
	}
	if err = data.Set(serverProp, server); err != nil {
		return nil, err
	}

	parts := strings.Split(u.Path, "/")
	if len(parts) != 3 {
		return nil, errors.New("invalid ID")
	}
	if err = data.Set(databaseProp, parts[1]); err != nil {
		return nil, err
	}
	if err = data.Set(schemaNameProp, parts[2]); err != nil {
		return nil, err
	}

	data.SetId(getSchemaID(data))

	database := data.Get(databaseProp).(string)
	schemaname := data.Get(schemaNameProp).(string)

	connector, err := getSchemaConnector(meta, data)
	if err != nil {
		return nil, err
	}

	schemaobj, err := connector.GetSchema(ctx, database, schemaname)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read schema [%s].[%s] for import", database, schemaname)
	}

	if schemaobj == nil {
		return nil, errors.Errorf("no schema [%s].[%s] found for import", database, schemaname)
	}

	if err = data.Set(schemaNameProp, schemaobj.SchemaName); err != nil {
		return nil, err
	}
	if err = data.Set(schemaIdProp, schemaobj.SchemaID); err != nil {
		return nil, err
	}
	if err = data.Set(principalIdProp, schemaobj.PrincipalID); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{data}, nil
}

func getSchemaConnector(meta interface{}, data *schema.ResourceData) (SchemaConnector, error) {
	provider := meta.(model.Provider)
	connector, err := provider.GetConnector(serverProp, data)
	if err != nil {
		return nil, err
	}
	return connector.(SchemaConnector), nil
}

