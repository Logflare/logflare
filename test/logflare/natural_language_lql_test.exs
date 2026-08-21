defmodule Logflare.NaturalLanguageLqlTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.NaturalLanguageLql
  alias Logflare.NaturalLanguageLql.AnthropicClient

  setup do
    %{source: build(:source, id: System.unique_integer([:positive]))}
  end

  test "includes the request and source fields in the prompt" do
    request = "find traces for the distinctive deployment region"

    source =
      source_with_schema(%{
        "event_message" => :string,
        "metadata.deployment.distinctive_region" => :string,
        "timestamp" => :datetime
      })

    expect(AnthropicClient, :generate, fn prompt ->
      assert prompt =~ request
      assert prompt =~ "metadata.deployment.distinctive_region: string"
      provider_query("event_message:error t:yesterday")
    end)

    assert {:ok, %{lql: ~s|~"(?i)error" t:yesterday|}} = generate(source, request)
  end

  test "returns the provider request ID when available", %{source: source} do
    for {expected_request_id, opts} <- [{"request-123", [request_id: "request-123"]}, {nil, []}] do
      expect_response(provider_query("event_message:error", opts))

      assert {:ok, %{lql: ~s|~"(?i)error"|, provider_request_id: ^expected_request_id}} =
               generate(source, "errors")
    end
  end

  test "returns a useful error when the provider cannot create a query", %{source: source} do
    expect_response(provider_error("unsupported"))

    assert {:error, :unsupported,
            "This request needs more detail or cannot be answered from this source's logs."} =
             generate(source, "orders for")
  end

  test "does not retry an invalid provider response", %{source: source} do
    expect_response(provider_response(%{kind: "query", lql: 123, error: nil}))

    assert {:error, :unavailable, "Natural-language query generation is currently unavailable."} =
             generate(source, "errors")
  end

  test "retries invalid LQL", %{source: source} do
    request = "find the relevant errors"

    for invalid_lql <- ["event_message:", "", "f:other_logs event_message:error", "s:not_a_field"] do
      expect(AnthropicClient, :generate, fn initial_prompt ->
        assert initial_prompt =~ request
        send(self(), {:initial_prompt, initial_prompt})
        provider_query(invalid_lql)
      end)

      expect(AnthropicClient, :generate, fn repair_prompt ->
        assert_receive {:initial_prompt, initial_prompt}
        assert repair_prompt =~ request
        refute initial_prompt == repair_prompt
        provider_query("event_message:error")
      end)

      assert {:ok, %{lql: ~s|~"(?i)error"|}} = generate(source, request)
    end
  end

  test "stops after three invalid LQL responses", %{source: source} do
    expect(AnthropicClient, :generate, 3, fn _prompt -> provider_query("f:other_logs") end)

    assert {:error, :unable_to_generate_valid_lql, "Could not generate a valid log query."} =
             generate(source, "errors")
  end

  test "does not retry when the provider is unavailable", %{source: source} do
    expect_response({:error, :unavailable})

    assert {:error, :unavailable, "Natural-language query generation is currently unavailable."} =
             generate(source, "errors")
  end

  test "makes string filters case-insensitive and leaves other filters unchanged" do
    schema = %{
      "event_message" => :string,
      "metadata.active" => :boolean,
      "metadata.city" => :string,
      "metadata.count" => :integer,
      "metadata.store.city" => :string,
      "metadata.tags" => {:list, :string},
      "timestamp" => :datetime
    }

    source = source_with_schema(schema)

    lql =
      ~s'"Failed (hard)" ~"(?i)error|warning$" m.store.city:"Phoenix (AZ)" m.city:~"Pho(enix)?$" m.active:true m.count:42 m.tags:@>~"VIP|staff$"'

    expect_response(provider_query(lql))

    assert {:ok, %{lql: canonical_lql}} = generate(source, "errors in Phoenix")
    assert {:ok, rules} = Lql.decode(canonical_lql, schema)

    message_rules = Enum.filter(rules, &match?(%FilterRule{path: "event_message"}, &1))
    assert Enum.any?(message_rules, &Regex.match?(Regex.compile!(&1.value), "FAILED (HARD)"))
    assert Enum.any?(message_rules, &Regex.match?(Regex.compile!(&1.value), "warning"))
    refute Enum.any?(message_rules, &String.starts_with?(&1.value, "(?i)(?i)"))

    assert %FilterRule{operator: :"~", value: store_pattern} =
             find_filter(rules, "metadata.store.city")

    store_regex = Regex.compile!(store_pattern)
    assert Regex.match?(store_regex, "phoenix (az)")
    refute Regex.match?(store_regex, "north phoenix (az)")

    assert %FilterRule{operator: :"~", value: "(?i)Pho(enix)?$"} =
             find_filter(rules, "metadata.city")

    assert %FilterRule{operator: :list_includes_regexp, value: "(?i)VIP|staff$"} =
             find_filter(rules, "metadata.tags")

    assert %FilterRule{operator: :=, value: true} = find_filter(rules, "metadata.active")
    assert %FilterRule{operator: :=, value: 42} = find_filter(rules, "metadata.count")
  end

  test "adds valid recommended fields without replacing custom aliases" do
    schema = %{
      "event_message" => :string,
      "metadata.count" => :integer,
      "metadata.missing" => :map,
      "metadata.request-method" => :string,
      "metadata.store.city" => :string,
      "timestamp" => :datetime
    }

    source =
      source_with_schema(schema,
        bigquery_clustering_fields: "event_message",
        suggested_keys: "metadata.store.city!,m.count,metadata.missing,metadata.request-method"
      )

    expect_response(provider_query("s:m.store.city@location"))

    assert {:ok, %{lql: lql}} = generate(source, "failed stores")
    assert {:ok, rules} = Lql.decode(lql, schema)

    selections =
      for %SelectRule{path: path, alias: alias_name} <- rules, into: %{} do
        {path, alias_name}
      end

    assert selections == %{
             "event_message" => "event_message",
             "metadata.count" => "count",
             "metadata.store.city" => "location"
           }
  end

  defp generate(source, request), do: NaturalLanguageLql.generate(source, request)

  defp expect_response(response),
    do: expect(AnthropicClient, :generate, fn _prompt -> response end)

  defp provider_query(lql, opts \\ []) do
    provider_response(%{kind: "query", lql: lql, error: nil}, opts)
  end

  defp provider_error(error), do: provider_response(%{kind: "error", lql: nil, error: error})

  defp provider_response(payload, opts \\ []) do
    response = %{text: Jason.encode!(payload)}

    response =
      case Keyword.fetch(opts, :request_id) do
        :error -> response
        {:ok, request_id} -> Map.put(response, :request_id, request_id)
      end

    {:ok, response}
  end

  defp source_with_schema(schema, attrs \\ []) do
    source = insert(:source, Keyword.put_new(attrs, :user, insert(:user)))
    insert(:source_schema, source: source, schema_flat_map: schema)
    source
  end

  defp find_filter(rules, path) do
    Enum.find(rules, &match?(%FilterRule{path: ^path}, &1))
  end
end
