# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/bq_schema_change_validate_bench.exs
#
# Env:
#   SAVE_SNAPSHOT=1     — append this run's results to bq_schema_change_validate_bench.history.exs
#   LABEL="..."         — optional label for the new entry
#   MACHINE="..."       — optional machine identifier so cross-machine entries can be filtered
#   PROFILE=1           — run :tprof against each scenario after the benchmark (Benchee profile_after)
#   TPROF_TYPE=time     — :tprof type when PROFILE=1 (time | calls | memory; default time)
#
# Targets BigQuerySchemaChange.validate/2 directly with a populated
# source_schema cache, so walk_map actually fires. log_event_make_bench's
# scenarios short-circuit on the empty-schema path, which hides any work
# inside the walk.

alias Logflare.Google.BigQuery.SchemaFactory
alias Logflare.Google.BigQuery.SchemaUtils
alias Logflare.LogEvent, as: LE
alias Logflare.Logs.Validators.BigQuerySchemaChange
alias Logflare.Profiling
alias Logflare.SourceSchemas
alias Logflare.Sources

import Logflare.Factory

# Sources.get_by/1 transitively hits Billing.get_plan_by/1, which raises if
# there's more than one Free plan. Insert only when missing — matches the
# pattern in log_event_make_bench.
Logflare.Repo.get_by(Logflare.Billing.Plan, name: "Free") || insert(:plan)

user = insert(:user)

build_source_with_schema = fn schema ->
  source = insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))

  SourceSchemas.create_or_update_source_schema(source, %{
    bigquery_schema: schema,
    schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
  })

  # Warm the cache so iteration 1 doesn't pay the cold miss.
  Logflare.SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

  source
end

# Scenario A: scalars only, 3 levels deep. Exercises walk_map / walk_entry /
# enforce_type on the happy path with no list branches.
schema_scalars = SchemaFactory.build(:schema, variant: :third)
source_scalars = build_source_with_schema.(schema_scalars)
metadata_scalars = SchemaFactory.build(:metadata, variant: :third)
le_scalars = LE.make(%{"metadata" => metadata_scalars}, %{source: source_scalars})

# Scenario B: same depth, plus REPEATED STRING / REPEATED INTEGER fields and
# a REPEATED RECORD (list of maps). Exercises walk_maps and the list branch
# of check_field.
schema_lists = SchemaFactory.build(:schema, variant: :third_with_lists)
source_lists = build_source_with_schema.(schema_lists)
metadata_lists = SchemaFactory.build(:metadata, variant: :third_with_lists)
le_lists = LE.make(%{"metadata" => metadata_lists}, %{source: source_lists})

# Scenario C: source.id set but no cached schema. The empty-schema clause in
# check_body fires and walk_map is never called — establishes the no-op floor.
source_empty = insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))
le_empty = LE.make(%{"metadata" => metadata_scalars}, %{source: source_empty})

profile_after =
  if System.get_env("PROFILE") == "1" do
    type = String.to_existing_atom(System.get_env("TPROF_TYPE") || "time")
    {:tprof, type: type, warmup: 0, sort: :per_call}
  else
    false
  end

suite =
  Benchee.run(
    %{
      "scalars (third)" => fn ->
        BigQuerySchemaChange.validate(le_scalars, source_scalars)
      end,
      "lists (third_with_lists)" => fn ->
        BigQuerySchemaChange.validate(le_lists, source_lists)
      end,
      "empty schema short-circuit" => fn ->
        BigQuerySchemaChange.validate(le_empty, source_empty)
      end
    },
    time: 5,
    warmup: 2,
    memory_time: 3,
    reduction_time: 3,
    profile_after: profile_after
  )

history_path = Path.expand("bq_schema_change_validate_bench.history.exs", __DIR__)
Profiling.track(suite, history_path)
