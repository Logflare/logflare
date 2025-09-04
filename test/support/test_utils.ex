defmodule Logflare.TestUtils do
  @moduledoc """
  Testing utilities. Globally aliased under the `TestUtils` namespace.
  """

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema
  alias GoogleApi.BigQuery.V2.Model.TableSchema

  alias Logflare.SingleTenant
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  @doc """
  Configures the following `:logflare` env keys:
  - :single_tenant gets set to true
  - :api_key is randomly set, simulating user setting api key through env var
  - :supabase_mode is set based on flag

  Options:
  - :seed_user - boolean - enable to seed default plan and user
  - :supabase_mode - enable to seed supabase data
  """
  defmacro setup_single_tenant(opts \\ []) do
    opts =
      Enum.into(opts, %{
        seed_user: false,
        supabase_mode: false,
        bigquery_project_id: random_string(),
        backend_type: :bigquery,
        pg_schema: nil
      })

    quote do
      unquote(setup_single_tenant_backend(opts))

      setup do
        initial_single_tenant = Application.get_env(:logflare, :single_tenant)
        Application.put_env(:logflare, :single_tenant, true)

        if unquote(opts.seed_user) do
          {:ok, _} = SingleTenant.create_default_plan()
          {:ok, _user} = SingleTenant.create_default_user()
        end

        if unquote(opts.supabase_mode) do
          initial_supabase_mode = Application.get_env(:logflare, :supabase_mode)
          Application.put_env(:logflare, :supabase_mode, true)
          on_exit(fn -> Application.put_env(:logflare, :supabase_mode, initial_supabase_mode) end)
        end

        initial_public_access_token = Application.get_env(:logflare, :public_access_token)
        initial_private_access_token = Application.get_env(:logflare, :private_access_token)
        Application.put_env(:logflare, :public_access_token, Logflare.TestUtils.random_string(12))

        Application.put_env(
          :logflare,
          :private_access_token,
          Logflare.TestUtils.random_string(12)
        )

        on_exit(fn ->
          Application.put_env(:logflare, :single_tenant, initial_single_tenant)
          Application.put_env(:logflare, :public_access_token, initial_public_access_token)
          Application.put_env(:logflare, :private_access_token, initial_private_access_token)
        end)

        :ok
      end
    end
  end

  defp setup_single_tenant_backend(%{backend_type: :postgres, pg_schema: schema}) do
    quote do
      setup do
        Goth
        |> reject(:fetch, 1)

        %{username: username, password: password, database: database, hostname: hostname} =
          Application.get_env(:logflare, Logflare.Repo) |> Map.new()

        url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"
        previous = Application.get_env(:logflare, :postgres_backend_adapter)

        Application.put_env(:logflare, :postgres_backend_adapter,
          url: url,
          schema: unquote(schema)
        )

        on_exit(fn -> Application.put_env(:logflare, :postgres_backend_adapter, previous) end)
        :ok
      end
    end
  end

  defp setup_single_tenant_backend(%{backend_type: :bigquery} = opts) do
    quote do
      setup do
        # conditionally update bigquery project id
        initial_google_config = Application.get_env(:logflare, Logflare.Google)
        replacement_project_id = unquote(opts.bigquery_project_id)
        updated = Keyword.put(initial_google_config, :project_id, replacement_project_id)
        Application.put_env(:logflare, Logflare.Google, updated)

        on_exit(fn -> Application.put_env(:logflare, Logflare.Google, initial_google_config) end)

        :ok
      end
    end
  end

  def default_bq_schema, do: SchemaBuilder.initial_table_schema()

  def build_bq_schema(params) do
    SchemaBuilder.build_table_schema(params, default_bq_schema())
  end

  @spec random_string(non_neg_integer()) :: String.t()
  def random_string(length \\ 6) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  def gen_bq_timestamp do
    micro = DateTime.utc_now() |> DateTime.to_unix(:microsecond)
    exp_first_part = micro / 1_000_000_000_000_000
    Float.to_string(exp_first_part) <> "E9"
  end

  @spec gen_uuid() :: String.t()
  def gen_uuid, do: Ecto.UUID.generate()

  @spec gen_uuid_atom() :: atom()
  def gen_uuid_atom, do: gen_uuid() |> String.to_atom()

  @spec gen_email() :: String.t()
  def gen_email, do: "#{random_string()}@#{random_string()}.com"

  @doc """
  Generates a mock BigQuery error
  ```elixir
  GoogleApi.BigQuery.V2.Api.Jobs
  |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
    {:ok, TestUtils.gen_bq_error("some-error")}
  end)
  ```
  """
  def gen_bq_error(err) do
    %Tesla.Env{
      status: 400,
      body: Jason.encode!(%{error: %{message: err}})
    }
  end

  @doc """
  Generates a mock BigQuery response.
  This is a successful bq response retrieved manually
  """
  def gen_bq_response(result \\ %{})

  def gen_bq_response(result) when is_map(result) do
    gen_bq_response([result])
  end

  def gen_bq_response(results) when is_list(results) do
    results =
      Enum.map(results, fn result ->
        result
        |> Enum.into(%{
          "event_message" => "some event message",
          "timestamp" => gen_bq_timestamp(),
          "id" => gen_uuid()
        })
      end)

    schema =
      if length(results) > 0 do
        SchemaBuilder.build_table_schema(results |> hd(), SchemaBuilder.initial_table_schema())
      else
        SchemaBuilder.initial_table_schema()
      end

    rows =
      for result <- results do
        row = %GoogleApi.BigQuery.V2.Model.TableRow{}

        cells =
          for field <- schema.fields do
            value = Map.get(result, field.name)
            %GoogleApi.BigQuery.V2.Model.TableCell{v: value}
          end

        %{row | f: cells}
      end

    %GoogleApi.BigQuery.V2.Model.QueryResponse{
      cacheHit: true,
      jobComplete: true,
      jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
        jobId: "job_eoaOXgp9U0VFOPiOHbX6fIT3z3KU",
        location: "US",
        projectId: "logflare-dev-238720"
      },
      kind: "bigquery#queryResponse",
      rows: rows,
      schema: schema,
      # Simple result length as test value
      totalBytesProcessed: length(rows) |> to_string(),
      totalRows: inspect(length(results))
    }
  end

  @doc """
  Used to retrieve a nested BigQuery field schema from a table schema. Returns nil if not found.

  ### Example
    iex> get_bq_field_schema(%TableSchema{...}, "metadata.a.b")
    %TableFieldSchema{...}
  """
  @spec get_bq_field_schema(TableSchema.t(), String.t()) :: nil | TableFieldSchema.t()
  def get_bq_field_schema(%TableSchema{} = schema, str_path) when is_binary(str_path) do
    str_path
    |> String.split(".")
    |> Enum.reduce(schema, fn
      _key, nil ->
        nil

      key, %_{fields: fields} ->
        Enum.find(fields, fn field -> field.name == key end)
    end)
  end

  @doc """
  Fixture for Gzipped request body from the Cloudflare Log Push HTTP service.
  Used for testing the NDJSON parser

  Gzipped:
    iex> cloduflare_log_push_body(decoded: false)

  Decoded:
    iex> cloduflare_log_push_body(decoded: true)
  """
  def cloudflare_log_push_body(decoded: false) do
    <<31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 229, 86, 91, 111, 226, 70, 20, 126, 239, 175, 168, 120,
      88, 181, 221, 197, 120, 124, 119, 164, 104, 69, 29, 178, 137, 148, 11, 2, 182, 91, 181, 170,
      170, 137, 125, 108, 166, 49, 51, 206, 204, 24, 138, 162, 253, 239, 61, 190, 96, 2, 36, 77,
      210, 174, 170, 149, 202, 3, 194, 199, 51, 231, 250, 125, 231, 227, 190, 23, 229, 12, 184,
      158, 192, 93, 9, 74, 143, 165, 208, 34, 22, 121, 239, 168, 119, 54, 155, 141, 7, 196, 32,
      189, 119, 237, 145, 233, 244, 34, 98, 197, 28, 36, 190, 28, 142, 134, 39, 253, 225, 104,
      106, 185, 94, 255, 67, 116, 217, 159, 158, 13, 237, 192, 233, 142, 182, 222, 62, 78, 206,
      241, 236, 64, 226, 207, 193, 146, 12, 114, 202, 19, 245, 126, 110, 255, 206, 146, 99, 184,
      51, 130, 128, 184, 52, 196, 107, 36, 173, 62, 120, 249, 211, 240, 20, 19, 72, 89, 14, 120,
      173, 228, 183, 92, 172, 120, 99, 158, 148, 57, 156, 159, 160, 21, 31, 71, 73, 6, 99, 170,
      231, 140, 103, 215, 5, 154, 86, 249, 126, 220, 9, 164, 32, 235, 60, 241, 205, 41, 147, 176,
      162, 121, 126, 73, 117, 60, 7, 213, 120, 82, 189, 163, 95, 127, 123, 215, 155, 66, 92, 74,
      166, 215, 23, 176, 132, 170, 230, 92, 172, 218, 0, 19, 80, 133, 224, 10, 126, 92, 107, 192,
      195, 1, 49, 55, 49, 134, 211, 171, 222, 17, 113, 60, 18, 236, 164, 50, 213, 84, 151, 120,
      178, 199, 229, 158, 139, 72, 112, 141, 247, 102, 235, 162, 42, 139, 22, 69, 206, 98, 170,
      153, 224, 131, 63, 148, 168, 234, 27, 83, 89, 229, 78, 215, 117, 133, 166, 89, 149, 44, 228,
      45, 200, 206, 167, 184, 237, 124, 214, 5, 158, 9, 165, 209, 156, 230, 41, 95, 243, 52, 203,
      230, 192, 21, 163, 169, 188, 91, 114, 67, 149, 5, 189, 161, 10, 140, 88, 180, 151, 166, 32,
      151, 32, 207, 199, 77, 63, 154, 42, 78, 96, 201, 98, 104, 115, 74, 64, 221, 106, 81, 116,
      113, 163, 241, 199, 25, 91, 224, 27, 18, 134, 102, 235, 67, 83, 169, 43, 163, 210, 116, 129,
      93, 39, 158, 231, 187, 196, 10, 45, 223, 177, 28, 179, 254, 116, 56, 145, 241, 88, 72, 204,
      207, 13, 66, 175, 179, 254, 220, 166, 14, 201, 39, 166, 231, 77, 42, 215, 146, 101, 140,
      239, 181, 218, 220, 183, 87, 64, 188, 160, 74, 95, 138, 132, 165, 12, 146, 3, 20, 96, 64,
      180, 45, 104, 44, 197, 62, 18, 94, 222, 168, 93, 26, 208, 58, 197, 93, 228, 182, 65, 35,
      145, 139, 106, 80, 182, 235, 224, 45, 138, 152, 218, 36, 186, 25, 151, 181, 237, 69, 235,
      175, 45, 205, 177, 77, 187, 189, 83, 127, 117, 243, 77, 214, 156, 46, 88, 140, 17, 126, 17,
      188, 6, 186, 227, 6, 158, 239, 91, 33, 233, 176, 80, 222, 200, 198, 89, 36, 74, 142, 69,
      145, 77, 140, 243, 113, 148, 83, 85, 67, 79, 76, 32, 22, 50, 233, 237, 37, 182, 1, 177, 229,
      108, 75, 136, 68, 82, 141, 254, 124, 120, 210, 48, 236, 52, 167, 89, 229, 195, 108, 30, 135,
      113, 133, 208, 29, 26, 238, 82, 91, 129, 28, 102, 80, 37, 130, 97, 19, 232, 167, 128, 244,
      194, 125, 97, 126, 251, 221, 219, 185, 214, 133, 58, 26, 12, 50, 156, 116, 121, 131, 253,
      93, 12, 110, 152, 102, 156, 15, 182, 71, 191, 63, 164, 102, 19, 179, 165, 102, 36, 196, 45,
      171, 178, 190, 255, 220, 66, 159, 106, 184, 96, 11, 166, 171, 246, 60, 10, 146, 209, 159, 5,
      58, 84, 15, 97, 142, 59, 235, 193, 74, 155, 93, 76, 151, 196, 176, 241, 237, 134, 108, 190,
      231, 4, 158, 227, 59, 94, 64, 110, 252, 102, 9, 85, 177, 70, 60, 121, 20, 235, 174, 231,
      110, 176, 190, 1, 24, 208, 4, 100, 155, 38, 246, 173, 41, 37, 249, 137, 202, 135, 105, 212,
      220, 179, 29, 195, 50, 137, 17, 134, 6, 113, 195, 222, 94, 81, 93, 191, 15, 104, 209, 48,
      177, 67, 84, 61, 125, 185, 174, 38, 163, 186, 179, 29, 183, 171, 169, 207, 24, 46, 191, 228,
      148, 229, 88, 114, 74, 115, 5, 221, 6, 189, 4, 165, 104, 6, 15, 51, 107, 203, 184, 4, 61,
      23, 21, 179, 198, 195, 89, 116, 214, 59, 4, 93, 231, 106, 111, 102, 83, 81, 202, 24, 218,
      153, 53, 201, 236, 246, 124, 139, 159, 221, 178, 54, 216, 55, 119, 151, 229, 14, 135, 186,
      225, 110, 155, 252, 249, 155, 251, 127, 166, 88, 87, 215, 87, 163, 103, 244, 41, 147, 64,
      117, 63, 19, 84, 171, 190, 164, 105, 154, 131, 122, 175, 32, 135, 88, 31, 255, 240, 134,
      226, 128, 150, 112, 204, 148, 161, 101, 9, 111, 16, 211, 69, 14, 184, 205, 42, 75, 221, 154,
      175, 65, 191, 68, 135, 224, 61, 234, 123, 166, 251, 95, 233, 87, 67, 41, 219, 114, 2, 43,
      13, 44, 47, 61, 84, 179, 109, 103, 14, 37, 77, 229, 113, 38, 16, 226, 197, 74, 223, 113,
      220, 34, 235, 187, 213, 51, 146, 70, 124, 203, 240, 77, 131, 248, 174, 97, 109, 71, 255, 50,
      125, 123, 70, 220, 136, 23, 120, 193, 19, 226, 246, 117, 8, 219, 11, 218, 245, 183, 194,
      246, 8, 228, 247, 100, 206, 33, 246, 107, 101, 206, 117, 2, 242, 197, 100, 206, 124, 181,
      204, 145, 192, 114, 255, 223, 58, 199, 177, 187, 7, 34, 103, 91, 158, 147, 38, 45, 35, 159,
      22, 57, 226, 133, 158, 255, 47, 68, 206, 176, 108, 215, 32, 150, 105, 56, 175, 149, 184,
      208, 218, 165, 218, 211, 74, 71, 108, 195, 65, 198, 187, 30, 6, 11, 191, 136, 236, 125, 24,
      205, 30, 21, 189, 106, 215, 191, 94, 243, 182, 255, 51, 30, 215, 188, 154, 57, 175, 82, 189,
      191, 0, 29, 159, 90, 32, 167, 13, 0, 0>>
  end

  def cloudflare_log_push_body(decoded: true) do
    %{
      "batch" => [
        %{
          "event_message" =>
            "{\"ClientRequestProtocol\":\"HTTP/1.1\",\"ClientSSLCipher\":\"AEAD-AES256-GCM-SHA384\",\"ClientRequestURI\":\"/rest/v1/lands?h3_id=eq.8815a93841fffff\",\"WAFProfile\":\"unknown\",\"WAFRuleID\":\"\",\"EdgePathingOp\":\"wl\",\"ClientRequestReferer\":\"\",\"FirewallMatchesRuleIDs\":[],\"SecurityLevel\":\"low\",\"EdgeResponseBytes\":810,\"ClientASN\":14618,\"EdgePathingStatus\":\"nr\",\"EdgeResponseContentType\":\"application/json\",\"ParentRayID\":\"00\",\"WorkerStatus\":\"ok\",\"EdgeRequestHost\":\"flfnynfgghensiafrqvn.supabase.co\",\"EdgeServerIP\":\"\",\"ClientDeviceType\":\"desktop\",\"WorkerCPUTime\":1990,\"EdgeStartTimestamp\":1667512927424000000,\"ClientSrcPort\":58960,\"ClientXRequestedWith\":\"\",\"OriginResponseBytes\":0,\"OriginResponseHTTPLastModified\":\"\",\"EdgePathingSrc\":\"macro\",\"ClientRequestHost\":\"flfnynfgghensiafrqvn.supabase.co\",\"ClientRequestPath\":\"/rest/v1/lands\",\"EdgeColoID\":354,\"CacheResponseStatus\":200,\"ClientRequestBytes\":4303,\"CacheCacheStatus\":\"dynamic\",\"ZoneID\":458677291,\"WorkerSubrequestCount\":1,\"ClientIPClass\":\"noRecord\",\"CacheResponseBytes\":824,\"EdgeColoCode\":\"IAD\",\"WAFFlags\":\"0\",\"WAFAction\":\"unknown\",\"ClientRequestUserAgent\":\"node-fetch/1.0 (+https://github.com/bitinn/node-fetch)\",\"FirewallMatchesActions\":[],\"Cookies\":{},\"EdgeRateLimitID\":0,\"OriginResponseHTTPExpires\":\"\",\"ClientSSLProtocol\":\"TLSv1.3\",\"RayID\":\"76486474681b7fff\",\"EdgeEndTimestamp\":1667512927565000000,\"RequestHeaders\":{},\"WAFMatchedVar\":\"\",\"ClientIP\":\"34.201.99.159\",\"EdgeRateLimitAction\":\"\",\"OriginResponseTime\":0,\"ClientCountry\":\"us\",\"OriginIP\":\"\",\"CacheTieredFill\":false,\"WAFRuleMessage\":\"\",\"ClientRequestMethod\":\"PATCH\",\"WorkerSubrequest\":false,\"FirewallMatchesSources\":[],\"OriginSSLProtocol\":\"unknown\",\"OriginResponseStatus\":0,\"EdgeResponseStatus\":200,\"ResponseHeaders\":{}}",
          "metadata" => %{
            "ClientRequestProtocol" => "HTTP/1.1",
            "ClientSSLCipher" => "AEAD-AES256-GCM-SHA384",
            "ClientRequestURI" => "/rest/v1/lands?h3_id=eq.8815a93841fffff",
            "WAFProfile" => "unknown",
            "WAFRuleID" => "",
            "EdgePathingOp" => "wl",
            "ClientRequestReferer" => "",
            "FirewallMatchesRuleIDs" => [],
            "SecurityLevel" => "low",
            "EdgeResponseBytes" => 810,
            "ClientASN" => 14_618,
            "EdgePathingStatus" => "nr",
            "EdgeResponseContentType" => "application/json",
            "ParentRayID" => "00",
            "WorkerStatus" => "ok",
            "EdgeRequestHost" => "flfnynfgghensiafrqvn.supabase.co",
            "EdgeServerIP" => "",
            "ClientDeviceType" => "desktop",
            "WorkerCPUTime" => 1990,
            "EdgeStartTimestamp" => 1_667_512_927_424_000_000,
            "ClientSrcPort" => 58_960,
            "ClientXRequestedWith" => "",
            "OriginResponseBytes" => 0,
            "OriginResponseHTTPLastModified" => "",
            "EdgePathingSrc" => "macro",
            "ClientRequestHost" => "flfnynfgghensiafrqvn.supabase.co",
            "ClientRequestPath" => "/rest/v1/lands",
            "EdgeColoID" => 354,
            "CacheResponseStatus" => 200,
            "ClientRequestBytes" => 4303,
            "CacheCacheStatus" => "dynamic",
            "ZoneID" => 458_677_291,
            "WorkerSubrequestCount" => 1,
            "ClientIPClass" => "noRecord",
            "CacheResponseBytes" => 824,
            "EdgeColoCode" => "IAD",
            "WAFFlags" => "0",
            "WAFAction" => "unknown",
            "ClientRequestUserAgent" => "node-fetch/1.0 (+https://github.com/bitinn/node-fetch)",
            "FirewallMatchesActions" => [],
            "Cookies" => %{},
            "EdgeRateLimitID" => 0,
            "OriginResponseHTTPExpires" => "",
            "ClientSSLProtocol" => "TLSv1.3",
            "RayID" => "76486474681b7fff",
            "EdgeEndTimestamp" => 1_667_512_927_565_000_000,
            "RequestHeaders" => %{},
            "WAFMatchedVar" => "",
            "ClientIP" => "34.201.99.159",
            "EdgeRateLimitAction" => "",
            "OriginResponseTime" => 0,
            "ClientCountry" => "us",
            "OriginIP" => "",
            "CacheTieredFill" => false,
            "WAFRuleMessage" => "",
            "ClientRequestMethod" => "PATCH",
            "WorkerSubrequest" => false,
            "FirewallMatchesSources" => [],
            "OriginSSLProtocol" => "unknown",
            "OriginResponseStatus" => 0,
            "EdgeResponseStatus" => 200,
            "ResponseHeaders" => %{}
          },
          "timestamp" => 1_667_512_927_424_000_000
        },
        %{
          "event_message" =>
            "{\"ClientRequestProtocol\":\"HTTP/1.1\",\"ClientSSLCipher\":\"NONE\",\"ClientRequestURI\":\"/rest/v1/great-goats-raffles?select=*&active=is.true&completed=is.false\",\"WAFProfile\":\"unknown\",\"WAFRuleID\":\"\",\"EdgePathingOp\":\"wl\",\"ClientRequestReferer\":\"\",\"FirewallMatchesRuleIDs\":[],\"SecurityLevel\":\"off\",\"EdgeResponseBytes\":605,\"ClientASN\":14618,\"EdgePathingStatus\":\"nr\",\"EdgeResponseContentType\":\"application/json\",\"ParentRayID\":\"76486432482f826f\",\"WorkerStatus\":\"unknown\",\"EdgeRequestHost\":\"slcgoounpwtqntpsyqwn.supabase.co\",\"EdgeServerIP\":\"172.70.175.21\",\"ClientDeviceType\":\"desktop\",\"WorkerCPUTime\":0,\"EdgeStartTimestamp\":1667512916868000000,\"ClientSrcPort\":0,\"ClientXRequestedWith\":\"\",\"OriginResponseBytes\":0,\"OriginResponseHTTPLastModified\":\"\",\"EdgePathingSrc\":\"macro\",\"ClientRequestHost\":\"slcgoounpwtqntpsyqwn.supabase.co\",\"ClientRequestPath\":\"/rest/v1/great-goats-raffles\",\"EdgeColoID\":413,\"CacheResponseStatus\":200,\"ClientRequestBytes\":5481,\"CacheCacheStatus\":\"dynamic\",\"ZoneID\":458677291,\"WorkerSubrequestCount\":0,\"ClientIPClass\":\"noRecord\",\"CacheResponseBytes\":1825,\"EdgeColoCode\":\"IAD\",\"WAFFlags\":\"0\",\"WAFAction\":\"unknown\",\"ClientRequestUserAgent\":\"node-fetch/1.0 (+https://github.com/bitinn/node-fetch)\",\"FirewallMatchesActions\":[],\"Cookies\":{},\"EdgeRateLimitID\":0,\"OriginResponseHTTPExpires\":\"\",\"ClientSSLProtocol\":\"none\",\"RayID\":\"7648643264fd826f\",\"EdgeEndTimestamp\":1667512916967000000,\"RequestHeaders\":{},\"WAFMatchedVar\":\"\",\"ClientIP\":\"3.235.120.4\",\"EdgeRateLimitAction\":\"\",\"OriginResponseTime\":92000000,\"ClientCountry\":\"us\",\"OriginIP\":\"13.40.156.239\",\"CacheTieredFill\":false,\"WAFRuleMessage\":\"\",\"ClientRequestMethod\":\"GET\",\"WorkerSubrequest\":true,\"FirewallMatchesSources\":[],\"OriginSSLProtocol\":\"TLSv1.3\",\"OriginResponseStatus\":200,\"EdgeResponseStatus\":200,\"ResponseHeaders\":{}}",
          "metadata" => %{
            "ClientRequestProtocol" => "HTTP/1.1",
            "ClientSSLCipher" => "NONE",
            "ClientRequestURI" =>
              "/rest/v1/great-goats-raffles?select=*&active=is.true&completed=is.false",
            "WAFProfile" => "unknown",
            "WAFRuleID" => "",
            "EdgePathingOp" => "wl",
            "ClientRequestReferer" => "",
            "FirewallMatchesRuleIDs" => [],
            "SecurityLevel" => "off",
            "EdgeResponseBytes" => 605,
            "ClientASN" => 14_618,
            "EdgePathingStatus" => "nr",
            "EdgeResponseContentType" => "application/json",
            "ParentRayID" => "76486432482f826f",
            "WorkerStatus" => "unknown",
            "EdgeRequestHost" => "slcgoounpwtqntpsyqwn.supabase.co",
            "EdgeServerIP" => "172.70.175.21",
            "ClientDeviceType" => "desktop",
            "WorkerCPUTime" => 0,
            "EdgeStartTimestamp" => 1_667_512_916_868_000_000,
            "ClientSrcPort" => 0,
            "ClientXRequestedWith" => "",
            "OriginResponseBytes" => 0,
            "OriginResponseHTTPLastModified" => "",
            "EdgePathingSrc" => "macro",
            "ClientRequestHost" => "slcgoounpwtqntpsyqwn.supabase.co",
            "ClientRequestPath" => "/rest/v1/great-goats-raffles",
            "EdgeColoID" => 413,
            "CacheResponseStatus" => 200,
            "ClientRequestBytes" => 5481,
            "CacheCacheStatus" => "dynamic",
            "ZoneID" => 458_677_291,
            "WorkerSubrequestCount" => 0,
            "ClientIPClass" => "noRecord",
            "CacheResponseBytes" => 1825,
            "EdgeColoCode" => "IAD",
            "WAFFlags" => "0",
            "WAFAction" => "unknown",
            "ClientRequestUserAgent" => "node-fetch/1.0 (+https://github.com/bitinn/node-fetch)",
            "FirewallMatchesActions" => [],
            "Cookies" => %{},
            "EdgeRateLimitID" => 0,
            "OriginResponseHTTPExpires" => "",
            "ClientSSLProtocol" => "none",
            "RayID" => "7648643264fd826f",
            "EdgeEndTimestamp" => 1_667_512_916_967_000_000,
            "RequestHeaders" => %{},
            "WAFMatchedVar" => "",
            "ClientIP" => "3.235.120.4",
            "EdgeRateLimitAction" => "",
            "OriginResponseTime" => 92_000_000,
            "ClientCountry" => "us",
            "OriginIP" => "13.40.156.239",
            "CacheTieredFill" => false,
            "WAFRuleMessage" => "",
            "ClientRequestMethod" => "GET",
            "WorkerSubrequest" => true,
            "FirewallMatchesSources" => [],
            "OriginSSLProtocol" => "TLSv1.3",
            "OriginResponseStatus" => 200,
            "EdgeResponseStatus" => 200,
            "ResponseHeaders" => %{}
          },
          "timestamp" => 1_667_512_916_868_000_000
        }
      ]
    }
  end

  def random_pos_integer(limit \\ 1000), do: :rand.uniform(limit)

  @doc """
  Run function `times` times and will retry failed assertions
  """
  @spec retry_assert(opts :: keyword(), func :: (-> any())) :: any()
  def retry_assert(opts \\ [], func) do
    sleep = opts[:sleep] || 50
    total = opts[:duration] || 5_000

    do_retry_assert(sleep, total, func)
  end

  defp do_retry_assert(sleep, total, func) do
    func.()
  rescue
    error in ExUnit.AssertionError ->
      left = total - sleep

      if left > 0 do
        :timer.sleep(sleep)
        do_retry_assert(sleep, left, func)
      else
        reraise error, __STACKTRACE__
      end
  end

  @doc """
  `Phoenix.LiveViewTest` has `open_browser/2` function that opens a browser with
  the given HTML content. This is kinda the same but for Phoenix static views.

  ## Usage

  You can call this with any HTML content.

  ```elixir
  TestUtils.open_browser("<html><body>Hello World</body></html>")
  ```

  However, it is mostly useful in tests.

  ```elixir
  conn
  |> get("/some/path")
  |> html_response(200)
  |> TestUtils.open_browser()
  ```
  """
  def open_browser(html, filename \\ "/tmp/test-#{System.unique_integer([:positive])}.html") do
    File.write!(filename, html)

    case :os.type() do
      {:unix, :darwin} -> System.cmd("open", [filename])
      {:unix, _} -> System.cmd("xdg-open", [filename])
      {:win32, _} -> System.cmd("cmd", ["/c", "start", filename])
    end

    html
  end

  @doc """
  Attaches a telemetry forwarder that sends events to the current test process.
  """
  @spec attach_forwarder(event_name :: [atom()], opts :: Keyword.t()) :: String.t()
  def attach_forwarder(event_name, opts \\ []) do
    test_pid = Keyword.get(opts, :pid, self())
    id = "test-telemetry-" <> Base.encode16(:erlang.term_to_binary(make_ref()))

    :ok =
      :telemetry.attach(
        id,
        event_name,
        fn ^event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event_name, measurements, metadata})
        end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    id
  end
end
