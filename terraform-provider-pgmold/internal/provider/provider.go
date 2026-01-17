package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ provider.Provider = &PgmoldProvider{}

type PgmoldProvider struct {
	version string
}

type PgmoldProviderModel struct {
	PgmoldBinary types.String `tfsdk:"pgmold_binary"`
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &PgmoldProvider{
			version: version,
		}
	}
}

func (p *PgmoldProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "pgmold"
	resp.Version = p.version
}

func (p *PgmoldProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Terraform provider for pgmold - PostgreSQL schema-as-code tool",
		Attributes: map[string]schema.Attribute{
			"pgmold_binary": schema.StringAttribute{
				Description: "Path to the pgmold binary. Defaults to 'pgmold' (assumes it's in PATH).",
				Optional:    true,
			},
		},
	}
}

func (p *PgmoldProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config PgmoldProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	pgmoldBinary := "pgmold"
	if !config.PgmoldBinary.IsNull() {
		pgmoldBinary = config.PgmoldBinary.ValueString()
	}

	providerData := &ProviderData{
		PgmoldBinary: pgmoldBinary,
	}

	resp.DataSourceData = providerData
	resp.ResourceData = providerData
}

func (p *PgmoldProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewSchemaResource,
	}
}

func (p *PgmoldProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

type ProviderData struct {
	PgmoldBinary string
}
