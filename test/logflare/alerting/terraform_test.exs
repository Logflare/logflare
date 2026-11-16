defmodule Logflare.Alerting.TerraformTest do
  use ExUnit.Case, async: true

  alias Logflare.Alerting.AlertQuery
  alias Logflare.Alerting.Terraform

  @rule %AlertQuery{
    name: "Edge Function errors",
    description: "An Edge Function returned 5xx responses.",
    language: :pg_sql,
    query: "select function_id, count(*) as errors\nfrom function_edge_logs\ngroup by 1",
    cron: "*/5 * * * *",
    rule_key: "supabase.edge-function-5xx",
    managed: true,
    severity: :major,
    category: :health,
    group_by: ["function_id"],
    scope: %{"project" => "default"},
    resolve_after_runs: 2,
    enabled: true,
    signal_mapping: %{
      "preview" => %{"lql" => "f:function_edge_logs m.function_id:{{function_id}}"},
      "title_column" => "title",
      "metadata_columns" => ["remediation"],
      "notebook" => [
        %{"title" => "Errors", "source" => "database", "sql" => "select 1;"}
      ]
    }
  }

  test "renders an alert issue rule as a logflare_alert resource" do
    assert Terraform.to_hcl(@rule) == ~S"""
           resource "logflare_alert" "supabase_edge_function_5xx" {
             name               = "Edge Function errors"
             description        = "An Edge Function returned 5xx responses."
             language           = "pg_sql"
             query              = <<-SQL
               select function_id, count(*) as errors
               from function_edge_logs
               group by 1
             SQL
             cron               = "*/5 * * * *"
             rule_key           = "supabase.edge-function-5xx"
             managed            = true
             severity           = "major"
             category           = "health"
             group_by           = ["function_id"]
             scope              = { project = "default" }
             resolve_after_runs = 2
             enabled            = true
             signal_mapping = {
               title_column     = "title"
               metadata_columns = ["remediation"]
               preview          = { lql = "f:function_edge_logs m.function_id:{{function_id}}" }
               notebook = [
                 { title = "Errors", source = "database", sql = "select 1;" },
               ]
             }
           }
           """
  end

  test "omits unset optional attributes and keeps single-line queries inline" do
    hcl =
      Terraform.to_hcl(%AlertQuery{
        name: "Plain",
        language: :lql,
        query: "f:auth_logs m.status:>=400",
        cron: "* * * * *",
        rule_key: "custom.plain",
        managed: false,
        severity: :warning,
        category: :security,
        group_by: [],
        scope: %{},
        resolve_after_runs: 2,
        enabled: true,
        signal_mapping: %{}
      })

    assert hcl =~ ~s(  query              = "f:auth_logs m.status:>=400"\n)
    refute hcl =~ "description"
    refute hcl =~ "group_by"
    refute hcl =~ "scope"
    refute hcl =~ "signal_mapping"
  end

  test "escapes strings and template sequences" do
    hcl =
      Terraform.to_hcl(%AlertQuery{
        name: ~S(say "hi" \ ${var} %{if}),
        query: "select '${x}'\nfrom t",
        language: :pg_sql,
        cron: "* * * * *",
        rule_key: "custom.esc",
        enabled: true
      })

    assert hcl =~ ~S(name               = "say \"hi\" \\ $${var} %%{if}")
    assert hcl =~ ~S(    select '$${x}')
  end

  test "quotes map keys that are not identifiers and labels from the name without a rule key" do
    hcl =
      Terraform.to_hcl(%AlertQuery{
        name: "1st Rule!",
        query: "select 1",
        language: :pg_sql,
        cron: "* * * * *",
        enabled: true,
        scope: %{"my-key" => "v"}
      })

    assert hcl =~ ~s(resource "logflare_alert" "alert_1st_rule_" {)
    assert hcl =~ ~s(scope              = { "my-key" = "v" })
  end
end
