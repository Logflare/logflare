defmodule Logflare.EnrichmentRulesTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.EnrichmentRules
  alias Logflare.EnrichmentRules.EnrichmentRule
  alias Logflare.LogEvent
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    [user: user, source: source]
  end

  describe "create/2" do
    test "stores the conditions and fields", %{source: source} do
      conditions = [
        %{"path" => "debug_mode", "predicate" => "equals", "value" => "errors_only"},
        %{"path" => "m.status", "predicate" => "less_than", "value" => 400}
      ]

      assert {:ok, %EnrichmentRule{} = rule} =
               EnrichmentRules.create(source, %{
                 "conditions" => conditions,
                 "set_fields" => %{"m.alert" => true}
               })

      assert rule.source_id == source.id
      assert rule.match == :all
      assert rule.set_fields == %{"m.alert" => true}
      assert [%EnrichmentRule{id: id} = stored] = EnrichmentRules.list_by_source_id(source.id)
      assert id == rule.id
      assert stored.conditions == conditions
    end

    test "rejects missing and invalid conditions", %{source: source} do
      for {conditions, message} <- [
            {[], "must contain at least one condition"},
            {[%{"path" => "a", "predicate" => "matches"}], "unknown predicate: matches"},
            {[%{"path" => "m..a", "predicate" => "exists"}], "invalid field path"},
            {[%{"path" => "a[0]", "predicate" => "exists"}], "invalid field path"},
            {[%{"path" => "a", "predicate" => "equals"}], "needs a string, number or boolean"},
            {[%{"path" => "a", "predicate" => "less_than", "value" => "4"}], "needs a number"},
            {[%{"path" => "a", "predicate" => "in", "value" => []}], "needs a list of values"},
            {[%{"predicate" => "exists"}], "must have a path and a predicate"}
          ] do
        assert {:error, changeset} =
                 EnrichmentRules.create(source, %{
                   "conditions" => conditions,
                   "set_fields" => %{"a" => 1}
                 })

        assert %{conditions: [error]} = errors_on(changeset)
        assert error =~ message
      end
    end

    test "rejects empty, reserved and non-scalar set fields", %{source: source} do
      for {set_fields, message} <- [
            {%{}, "must set at least one field"},
            {%{"timestamp" => 1}, "cannot set reserved field: timestamp"},
            {%{"m.tags" => ["a"]}, "value for m.tags must be a string, number or boolean"},
            {%{"metadata..x" => 1}, "has an invalid field path"}
          ] do
        assert {:error, changeset} =
                 EnrichmentRules.create(source, %{
                   "conditions" => [%{"path" => "a", "predicate" => "exists"}],
                   "set_fields" => set_fields
                 })

        assert %{set_fields: [error]} = errors_on(changeset)
        assert error =~ message
      end
    end
  end

  describe "apply_rules/2" do
    test "leaves events unchanged when the source has no rules", %{source: source} do
      events = [build(:log_event, source: source, message: "testing")]

      assert EnrichmentRules.apply_rules(events, source) == events
    end

    test "sets fields on matching events only", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        conditions: [%{"path" => "m.level", "predicate" => "equals", "value" => "error"}],
        set_fields: %{"alert" => true, "m.team" => "core"}
      )

      matching = build(:log_event, source: source, metadata: %{"level" => "error"})
      other = build(:log_event, source: source, metadata: %{"level" => "info"})

      assert [enriched, unchanged] = EnrichmentRules.apply_rules([matching, other], source)
      assert enriched.body["alert"] == true
      assert enriched.body["metadata"]["team"] == "core"
      assert enriched.body["metadata"]["level"] == "error"
      assert unchanged == other
    end

    test "flags non-error events from errors_only projects", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        conditions: [
          %{"path" => "debug_mode", "predicate" => "equals", "value" => "errors_only"},
          %{"path" => "m.response.status_code", "predicate" => "less_than", "value" => 400}
        ],
        set_fields: %{"excluded_by_user_config" => true}
      )

      events =
        for {mode, status} <- [{"errors_only", 200}, {"errors_only", 500}, {"all", 200}] do
          build(:log_event,
            source: source,
            debug_mode: mode,
            metadata: %{"response" => %{"status_code" => status}}
          )
        end

      assert [ok, error, all] = EnrichmentRules.apply_rules(events, source)
      assert ok.body["excluded_by_user_config"] == true
      refute Map.has_key?(error.body, "excluded_by_user_config")
      refute Map.has_key?(all.body, "excluded_by_user_config")
    end

    test "matches any condition and supports in, not_equals and presence predicates",
         %{source: source} do
      insert(:enrichment_rule,
        source: source,
        match: :any,
        conditions: [
          %{"path" => "m.level", "predicate" => "in", "value" => ["error", "fatal"]},
          %{"path" => "m.status", "predicate" => "greater_than", "value" => 499}
        ],
        set_fields: %{"page" => true}
      )

      insert(:enrichment_rule,
        source: source,
        conditions: [
          %{"path" => "m.user_id", "predicate" => "exists"},
          %{"path" => "m.env", "predicate" => "not_equals", "value" => "prod"}
        ],
        set_fields: %{"debug_user" => true}
      )

      events =
        for metadata <- [
              %{"level" => "fatal", "status" => 200},
              %{"level" => "info", "status" => 503},
              %{"level" => "info", "status" => 200, "user_id" => 1, "env" => "dev"},
              %{"level" => "info", "status" => 200, "user_id" => 1, "env" => "prod"}
            ] do
          build(:log_event, source: source, metadata: metadata)
        end

      assert [fatal, unavailable, dev_user, prod_user] =
               EnrichmentRules.apply_rules(events, source)

      assert fatal.body["page"] == true
      assert unavailable.body["page"] == true
      refute Map.has_key?(dev_user.body, "page")
      assert dev_user.body["debug_user"] == true
      refute Map.has_key?(prod_user.body, "debug_user")
    end

    test "applies matching rules in creation order", %{source: source} do
      insert(:enrichment_rule, source: source, set_fields: %{"tier" => "first"})
      insert(:enrichment_rule, source: source, set_fields: %{"tier" => "second"})

      [event] =
        EnrichmentRules.apply_rules(
          [build(:log_event, source: source, message: "testing")],
          source
        )

      assert event.body["tier"] == "second"
    end

    test "skips a path that crosses a non-map value", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        set_fields: %{"m.level.nested" => 1, "ok" => 1}
      )

      [event] =
        EnrichmentRules.apply_rules(
          [build(:log_event, source: source, message: "testing", metadata: %{"level" => "info"})],
          source
        )

      assert event.body["metadata"]["level"] == "info"
      assert event.body["ok"] == 1
    end

    test "leaves events routed in by another rule unchanged", %{source: source} do
      insert(:enrichment_rule, source: source)
      routed = %{build(:log_event, source: source, message: "testing") | via_rule_id: 1}

      assert EnrichmentRules.apply_rules([routed], source) == [routed]
    end

    test "skips the rules lookup when every event was routed in", %{source: source} do
      insert(:enrichment_rule, source: source)
      cache_key = {:compiled_by_source_id, [source.id]}
      routed = %{build(:log_event, source: source, message: "testing") | via_rule_id: 1}

      assert EnrichmentRules.apply_rules([routed, routed], source) == [routed, routed]
      assert Cachex.get!(EnrichmentRules.Cache, cache_key) == nil

      fresh = build(:log_event, source: source, message: "testing")
      assert [^routed, enriched] = EnrichmentRules.apply_rules([routed, fresh], source)
      assert enriched.body["enriched"] == true
      assert {:cached, %{}} = Cachex.get!(EnrichmentRules.Cache, cache_key)
    end

    test "resets cached byte sizes so metering measures the enriched body", %{source: source} do
      insert(:enrichment_rule, source: source)

      event = LogEvent.cache_sizes(build(:log_event, source: source, message: "testing"))
      [enriched] = EnrichmentRules.apply_rules([event], source)

      assert enriched.accounted_bytes == nil
      assert LogEvent.accounted_byte_size(enriched) > LogEvent.accounted_byte_size(event)
    end
  end

  describe "dispatch" do
    setup %{source: source} do
      start_supervised!(AllLogsLogged)
      start_supervised!({SourceSup, source})

      insert(:enrichment_rule,
        source: source,
        conditions: [%{"path" => "m.level", "predicate" => "equals", "value" => "error"}],
        set_fields: %{"alert" => true}
      )

      :ok
    end

    test "direct ingest enriches events before they are queued", %{source: source} do
      le = build(:log_event, source: source, metadata: %{"level" => "error"})

      assert {:ok, 1} = Backends.ingest_logs([le], source)
      assert [queued] = Backends.list_recent_logs_local(source)
      assert queued.body["alert"] == true
    end

    test "the spool consumer enriches events before they are queued", %{source: source} do
      record = %{
        "id" => Ecto.UUID.generate(),
        "body" => %{
          "message" => "spooled",
          "timestamp" => System.system_time(:microsecond),
          "metadata" => %{"level" => "error"}
        },
        "event_type" => "log",
        "ingested_at" => DateTime.to_iso8601(DateTime.utc_now())
      }

      assert {:ok, 1} = Backends.dispatch_from_spool([record], source)
      assert [queued] = Backends.list_recent_logs_local(source)
      assert queued.body["alert"] == true
    end
  end
end
