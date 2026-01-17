package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &SchemaResource{}
	_ resource.ResourceWithConfigure   = &SchemaResource{}
	_ resource.ResourceWithImportState = &SchemaResource{}
)

type SchemaResource struct {
	providerData *ProviderData
}

type SchemaResourceModel struct {
	ID               types.String `tfsdk:"id"`
	SchemaFile       types.String `tfsdk:"schema_file"`
	DatabaseURL      types.String `tfsdk:"database_url"`
	TargetSchemas    types.String `tfsdk:"target_schemas"`
	AllowDestructive types.Bool   `tfsdk:"allow_destructive"`
	ValidateURL      types.String `tfsdk:"validate_url"`
	SchemaHash       types.String `tfsdk:"schema_hash"`
	LastApplied      types.String `tfsdk:"last_applied"`
}

func NewSchemaResource() resource.Resource {
	return &SchemaResource{}
}

func (r *SchemaResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

func (r *SchemaResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a PostgreSQL database schema using pgmold.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "Unique identifier for this schema resource.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"schema_file": schema.StringAttribute{
				Required:    true,
				Description: "Path to the SQL schema file defining the desired database state.",
			},
			"database_url": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/dbname).",
			},
			"target_schemas": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("public"),
				Description: "Comma-separated list of PostgreSQL schemas to manage. Defaults to 'public'.",
			},
			"allow_destructive": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "Whether to allow destructive operations (DROP TABLE, DROP COLUMN, etc.).",
			},
			"validate_url": schema.StringAttribute{
				Optional:    true,
				Description: "Optional URL of a temporary database to validate migrations before applying.",
			},
			"schema_hash": schema.StringAttribute{
				Computed:    true,
				Description: "SHA256 hash of the schema file content, used to detect changes.",
			},
			"last_applied": schema.StringAttribute{
				Computed:    true,
				Description: "Timestamp of the last successful apply operation.",
			},
		},
	}
}

func (r *SchemaResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	providerData, ok := req.ProviderData.(*ProviderData)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *ProviderData, got: %T", req.ProviderData),
		)
		return
	}

	r.providerData = providerData
}

func (r *SchemaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan SchemaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	schemaContent, err := os.ReadFile(plan.SchemaFile.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Failed to read schema file", err.Error())
		return
	}

	hash := sha256.Sum256(schemaContent)
	schemaHash := hex.EncodeToString(hash[:])

	if err := r.runPgmoldApply(ctx, &plan); err != nil {
		resp.Diagnostics.AddError("Failed to apply schema", err.Error())
		return
	}

	plan.ID = types.StringValue(fmt.Sprintf("%s:%s", plan.DatabaseURL.ValueString(), plan.TargetSchemas.ValueString()))
	plan.SchemaHash = types.StringValue(schemaHash)
	plan.LastApplied = types.StringValue(currentTimestamp())

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *SchemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state SchemaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	schemaContent, err := os.ReadFile(state.SchemaFile.ValueString())
	if err != nil {
		tflog.Warn(ctx, "Schema file not found, resource may need to be recreated", map[string]interface{}{
			"file":  state.SchemaFile.ValueString(),
			"error": err.Error(),
		})
	} else {
		hash := sha256.Sum256(schemaContent)
		state.SchemaHash = types.StringValue(hex.EncodeToString(hash[:]))
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *SchemaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan SchemaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var state SchemaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	schemaContent, err := os.ReadFile(plan.SchemaFile.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Failed to read schema file", err.Error())
		return
	}

	hash := sha256.Sum256(schemaContent)
	newHash := hex.EncodeToString(hash[:])

	if newHash != state.SchemaHash.ValueString() || plan.DatabaseURL.ValueString() != state.DatabaseURL.ValueString() {
		if err := r.runPgmoldApply(ctx, &plan); err != nil {
			resp.Diagnostics.AddError("Failed to apply schema", err.Error())
			return
		}
		plan.LastApplied = types.StringValue(currentTimestamp())
	} else {
		plan.LastApplied = state.LastApplied
	}

	plan.ID = state.ID
	plan.SchemaHash = types.StringValue(newHash)

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *SchemaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	tflog.Info(ctx, "Deleting pgmold_schema resource. Note: Database objects are NOT dropped. Use allow_destructive with caution.")
}

func (r *SchemaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *SchemaResource) runPgmoldApply(ctx context.Context, model *SchemaResourceModel) error {
	args := []string{
		"apply",
		"--schema", model.SchemaFile.ValueString(),
		"--database", model.DatabaseURL.ValueString(),
		"--target-schemas", model.TargetSchemas.ValueString(),
	}

	if model.AllowDestructive.ValueBool() {
		args = append(args, "--allow-destructive")
	}

	if !model.ValidateURL.IsNull() && model.ValidateURL.ValueString() != "" {
		args = append(args, "--validate", model.ValidateURL.ValueString())
	}

	tflog.Debug(ctx, "Running pgmold", map[string]interface{}{
		"binary": r.providerData.PgmoldBinary,
		"args":   strings.Join(args, " "),
	})

	cmd := exec.CommandContext(ctx, r.providerData.PgmoldBinary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pgmold apply failed: %w\nOutput: %s", err, string(output))
	}

	tflog.Debug(ctx, "pgmold apply succeeded", map[string]interface{}{
		"output": string(output),
	})

	return nil
}

func currentTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
