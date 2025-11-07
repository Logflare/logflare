defmodule Logflare.SqlTest do
  use Logflare.DataCase

  import ExUnit.CaptureLog

  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.AdaptorSupervisor

  @logflare_project_id "logflare-project-id"
  @user_project_id "user-project-id"
  @user_dataset_id "user-dataset-id"
  @env "test"

  setup do
    insert(:plan)
    values = Application.get_env(:logflare, Logflare.Google)
    to_put = Keyword.put(values, :project_id, @logflare_project_id)
    Application.put_env(:logflare, Logflare.Google, to_put)

    on_exit(fn ->
      Application.put_env(:logflare, Logflare.Google, values)
    end)
  end

  describe "bigquery dialect" do
    test "parser can handle struct definitions" do
      user = insert(:user)

      for input <- [
            "select STRUCT(1,2,3)",
            "select STRUCT(\'abc\')",
            "select STRUCT(1, t.str_col)",
            "select STRUCT(str_col AS abc)"
            # Note: empty STRUCT() is not supported in sqlparser 0.39.0
          ] do
        assert {:ok, _v2} = Sql.transform(:bq_sql, input, user)
      end
    end

    test "parser can handle sandboxed CTEs with union all" do
      user = insert(:user)
      insert(:source, user: user, name: "my_table")

      # valid CTE queries with UNION ALL
      input = """
      with cte1 as (select a from my_table),
           cte2 as (select b from my_table),
           edge_logs as (select b from my_table),
           postgres_logs as (select b from my_table),
           auth_logs as (select b from my_table)
      select a from cte1
      union all
      select b from cte2
      union all
      \nselect el.id as id from edge_logs as el\nunion all\nselect pgl.id as id from postgres_logs as pgl\nunion all\nselect al.id as id from auth_logs as al
      """

      assert {:ok, _result} = Sql.transform(:bq_sql, input, user)
    end

    test "parser can handle complex sql" do
      user = insert(:user)

      for input <- [
            "select d[0]",
            "select d[offset(0)]"
          ] do
        assert {:ok, _v2} = Sql.transform(:bq_sql, input, user)
      end
    end

    test "non-BYOB - transforms table names correctly" do
      user = insert(:user)
      source = insert(:source, user: user, name: "my_table")
      source_dots = insert(:source, user: user, name: "my.table.name")
      source_other = insert(:source, user: user, name: "other_table")
      table = bq_table_name(source)
      table_other = bq_table_name(source_other)
      table_dots = bq_table_name(source_dots)

      for {input, expected} <- [
            # quoted
            {"select val from `my_table` where `my_table`.val > 5",
             "select val from #{table} where #{table}.val > 5"},
            #  source names with dots
            {"select val from `my.table.name` where `my.table.name`.val > 5",
             "select val from #{table_dots} where #{table_dots}.val > 5"},
            # joins
            {"select a from my_table join other_table as f on a = 123",
             "select a from #{table} join #{table_other} as f on a = 123"},
            # cross join + unnest with no join condition
            {"select a from my_table cross join unnest(my_table.col) as f",
             "select a from #{table} cross join unnest(#{table}.col) as f"},
            #  inner join  + unnest with join condition
            {"select a from my_table join unnest(my_table.col) on true",
             "select a from #{table} join unnest(#{table}.col) on true"},
            # where
            {"select val from my_table where my_table.val > 5",
             "select val from #{table} where #{table}.val > 5"},
            # select named column
            {"select val, my_table.abc from my_table", "select val, #{table}.abc from #{table}"},
            # group by
            {"select val from my_table group by my_table.val",
             "select val from #{table} group by #{table}.val"},
            #  order by
            {"select val from my_table order by my_table.val",
             "select val from #{table} order by #{table}.val"},
            #  CTE
            {"with src as (select n from `my_table`) select n from src",
             "with src as (select n from #{table}) select n from src"},
            # having
            {"select val from my_table group by my_table.abc having count(my_table.id) > 5",
             "select val from #{table} group by #{table}.abc having count(#{table}.id) > 5"},
            # alias
            {"select a from my_table as src where src.b > 5",
             "select a from #{table} as src where src.b > 5"},
            # joins
            {"select a from my_table left join other_table on my_table.d = other_table.e",
             "select a from #{table} left join #{table_other} on #{table}.d = #{table_other}.e"},
            # CTE with union
            {"with abc as (select val from my_table where val > 5) select val from abc union select a from other_table",
             "with abc as (select val from #{table} where val > 5) select val from abc union select a from #{table_other}"},
            # recursive CTE
            {"with src as (select a from my_table union select a from src) select a from src",
             "with src as (select a from #{table} union select a from src) select a from src"},
            # CTE referencing
            {
              "with src as (select a from my_table), abc as (select b from src) select c from abc union select a from src",
              "with src as (select a from #{table}), abc as (select b from src) select c from abc union select a from src"
            },
            # sandboxed queries
            {
              {"with src as (select a from my_table), src2 as (select a from src where a > 5) select c from src",
               "select a, b, c from src2"},
              "with src as (select a from #{table}), src2 as (select a from src where a > 5) select a, b, c from src2"
            },
            # sandboxed queries with order by
            {
              {"with src as (select a from my_table) select c from src",
               "select c from src order by c asc"},
              "with src as (select a from #{table}) select c from src order by c asc"
            },
            # sandboxed CTEs with union all
            {
              {"with cte1 as (select a from my_table), cte2 as (select b from my_table) select a from cte1",
               "select a from cte1 union all select b from cte2"},
              "with cte1 as (select a from #{table}), cte2 as (select b from #{table}) select a from cte1 union all select b from cte2"
            },
            # multiple union alls
            {
              {"with cte1 as (select a from my_table), cte2 as (select b from my_table) select a from cte1",
               "select a from cte1 union all select b from cte2 union all select c from cte2"},
              "with cte1 as (select a from #{table}), cte2 as (select b from #{table}) select a from cte1 union all select b from cte2 union all select c from cte2"
            },
            # handle nested CTEs
            {
              {"with cte1 as (select 'val' as a) select a from cte1",
               "with cte2 as (select 'val' as b) select a, b from cte1, cte2"},
              "with cte1 as (select 'val' as a) (with cte2 as (select 'val' as b) select a, b from cte1, cte2)"
            }
          ] do
        assert {:ok, v2} = Sql.transform(:bq_sql, input, user)
        assert String.downcase(v2) == expected
        assert {:ok, v2} = Sql.transform(:bq_sql, input, user.id)
        assert String.downcase(v2) == expected
      end

      # queries where v1 differs from v2, don't test for equality
      for {input, expected} <- [
            # subquery
            {"select a from (select b from my_table)", "select a from (select b from #{table})"}
          ] do
        assert {:ok, v2} = Sql.transform(:bq_sql, input, user)
        assert String.downcase(v2) == expected
      end
    end

    test "non-BYOB invalid queries" do
      user = insert(:user)
      insert(:source, user: user, name: "my_table")
      insert(:source, user: user, name: "other_table")

      # invalid queries
      for {input, expected} <- [
            # select-into queries are not parsed.
            {"SELECT a FROM a INTO b", "end of statement"},
            {
              {"with src as (select a from my_table) select c from src",
               "select a from src into src"},
              "end of statement"
            },
            # block no wildcard select
            {"select * from a", "restricted wildcard"},
            {"SELECT a.* FROM a", "restricted wildcard"},
            {"SELECT q, a.* FROM a", "restricted wildcard"},
            {"SELECT a FROM (SELECT * FROM a)", "restricted wildcard"},
            {"WITH q AS (SELECT a FROM a) SELECT * FROM q", "restricted wildcard"},
            {"SELECT a FROM a UNION ALL SELECT * FROM b", "restricted wildcard"},
            {
              {"with src as (select a from my_table) select c from src", "select * from src"},
              "restricted wildcard"
            },
            # sandbox: restricted table references not in CTE
            {
              {"with src as (select a from my_table) select c from src",
               "select a from my_table"},
              "Table not found in CTE: (my_table)"
            },
            # sandbox: restricted functions
            {"SELECT SESSION_USER()", "Restricted function session_user"},
            {"SELECT EXTERNAL_QUERY('','')", "Restricted function external_query"},
            {
              {"with src as (select a from my_table) select c from src", "select session_user()"},
              "Restricted function session_user"
            },
            {
              {"with src as (select a from my_table) select c from src",
               "select external_query('','')"},
              "Restricted function external_query"
            },
            # block DML
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
            {
              "insert a (x,y) values ('test', 5)",
              "Only SELECT queries allowed"
            },
            {
              "show policies on \"programs\"",
              "Only SELECT queries allowed"
            },
            {
              "update a set x = 1",
              "Only SELECT queries allowed"
            },
            {
              "delete from a where x = 1",
              "Only SELECT queries allowed"
            },
            {
              "truncate table a",
              "Only SELECT queries allowed"
            },
            {
              "MERGE t USING s ON t.product = s.product
                WHEN MATCHED THEN
                  UPDATE SET quantity = t.quantity + s.quantity
                WHEN NOT MATCHED THEN
                  INSERT (product, quantity) VALUES(product, quantity) ",
              "Only SELECT queries allowed"
            },
            {
              "drop table a",
              "Only SELECT queries allowed"
            },
            {{"with src as (select a from my_table) select c from src", "update a set x=2"},
             "Only SELECT queries allowed"},
            {{"with src as (select a from my_table) select c from src", "drop table a"},
             "Only SELECT queries allowed"},
            #  Block multiple queries
            {
              "select a from b; select c from d;",
              "Only singular query allowed"
            },
            {{"with src as (select a from my_table) select c from src",
              "select a from b; select c from d;"}, "Only singular query allowed"},
            {"select datetime() from `light-two-os-directions-test`",
             "can't find source light-two-os-directions-test"},
            {"with src as (select a from unknown_table) select datetime() from my_table",
             "can't find source unknown_table"},
            # cannot query logflare project
            {"select a from `#{@logflare_project_id}.mydataset.mytable`", "can't find source"},
            # fully qualified name that is not a source name should be rejected
            {"select a from `a.b.c`", "can't find source"}
          ] do
        assert {:error, err} = Sql.transform(:bq_sql, input, user)

        assert String.downcase(err) =~ String.downcase(expected),
               "should error with '#{expected}'. input: #{inspect(input)}"
      end
    end

    test "verify logging works for nil replacement_query scenarios" do
      user = insert(:user)
      insert(:source, user: user, name: "my_table")

      query = "with src as (select a from my_table) select c from src"
      sandboxed_query = "SHOW TABLES"

      logs =
        capture_log(fn ->
          result = Sql.transform(:bq_sql, {query, sandboxed_query}, user)
          assert {:error, "Only SELECT queries allowed in sandboxed queries"} = result

          # Check that the `:query_string` metadata is set
          metadata = Logger.metadata()
          assert Keyword.get(metadata, :query_string) == query
        end)

      assert logs =~
               "Sandboxed query validation: would produce nil replacement query. Transform count:"
    end
  end

  describe "Bigquery fully qualified" do
    test "able to use fully qualified names in queries" do
      user =
        insert(:user,
          bigquery_project_id: @user_project_id,
          bigquery_dataset_id: @user_dataset_id
        )

      source_abc = insert(:source, user: user, name: "a.b.c")
      source_cxy = insert(:source, user: user, name: "c.x.y")
      source_cxyz = insert(:source, user: user, name: "c.x.y.z")

      for {input, expected} <- [
            # fully qualified names must start with the user's bigquery project
            {"select a from `#{@user_project_id}.#{@user_dataset_id}.mytable`",
             "select a from `#{@user_project_id}`.`#{@user_dataset_id}`.`mytable`"},
            #  source names that look like dataset format
            {"select a from `a.b.c`", "select a from #{bq_table_name(source_abc)}"},
            {"with a as (select b from `c.x.y`) select b from a",
             "with a as (select b from #{bq_table_name(source_cxy)}) select b from a"},
            {"with a as (select b from `c.x.y.z`) select b from a",
             "with a as (select b from #{bq_table_name(source_cxyz)}) select b from a"}
          ] do
        assert Sql.transform(:bq_sql, input, user) |> elem(1) |> String.downcase() == expected
      end
    end

    # This test checks if a source name starting with a logflare project id will # get transformed correctly to the
    # source value this ensures  users cannot access other users' sources.
    test "source name replacement attack check - transform sources that have a fully-qualified name starting with global logflare project id" do
      user = insert(:user)
      source_name = "#{@logflare_project_id}.some.table"
      insert(:source, user: user, name: source_name)
      input = "select a from `#{source_name}`"

      assert {:ok, transformed} = Sql.transform(:bq_sql, input, user)
      refute transformed =~ source_name
    end
  end

  describe "bigquery single tenant - fully qualified name check" do
    @single_tenant_bq_project_id "single-tenant-id"
    TestUtils.setup_single_tenant(
      seed_user: true,
      bigquery_project_id: @single_tenant_bq_project_id
    )

    test "able to transform queries using provided bigquery project id" do
      user = SingleTenant.get_default_user()
      insert(:source, user: user, name: "a.b.c")
      input = " select a from `a.b.c`"

      assert {:ok, transformed} = Sql.transform(:bq_sql, input, user)
      assert transformed =~ @single_tenant_bq_project_id
      refute transformed =~ @logflare_project_id
    end

    test "allow user to set fully-qualified names" do
      user = SingleTenant.get_default_user()
      input = " select a from `#{@single_tenant_bq_project_id}.my_dataset.my_table`"

      assert {:ok, transformed} = Sql.transform(:bq_sql, input, user)
      assert transformed =~ "`#{@single_tenant_bq_project_id}`.`my_dataset`.`my_table`"
      refute transformed =~ @logflare_project_id
    end
  end

  describe "clickhouse dialect" do
    test "parser can handle tuple definitions" do
      user = insert(:user)

      for input <- [
            "select tuple(1,2,3) as foo",
            "select tuple(\'abc\')",
            "select tuple(1, t.str_col) from values(\'str_col String\', (\'hello\'), (\'world\'), (\'test\')) t",
            "SELECT tuple(t.str_col) AS abc from values(\'str_col String\', (\'hello\'), (\'world\'), (\'test\')) t"
          ] do
        assert {:ok, _v2} = Sql.transform(:ch_sql, input, user)
      end
    end

    test "parser can handle complex sql" do
      user = insert(:user)

      for input <- [
            "select d[0]",
            # Array indexing with expressions
            "select arr[1 + 2], arr[length(arr)] from values('arr Array(Int32)', ([10,20,30,40,50]))",
            # Map access with brackets
            "select map_col['key1'], map_col[concat('key', '2')] from (select map('key1', 100, 'key2', 200) as map_col)",
            # Array slicing
            "select arraySlice(arr, 2, 3) as slice_2_to_4, arraySlice(arr, 1, 3) as first_3, arraySlice(arr, 2) as from_2_onwards from values('arr Array(Int32)', ([1,2,3,4,5,6,7]))",
            # Nested access
            "select nested.names[1], nested.values[1] from values('nested Nested(names String, values Int32)', (['john', 'jane'], [25, 30]))"
          ] do
        assert {:ok, _v2} = Sql.transform(:ch_sql, input, user)
      end
    end

    test "parser can handle sandboxed CTEs with union all" do
      user = insert(:user)
      insert(:source, user: user, name: "my_ch_table")

      # valid CTE queries with UNION ALL
      input = """
      with cte1 as (select a from my_ch_table),
           cte2 as (select b from my_ch_table),
           edge_logs as (select b from my_ch_table),
           postgres_logs as (select b from my_ch_table),
           auth_logs as (select b from my_ch_table)
      select a from cte1
      union all
      select b from cte2
      union all
      \nselect el.id as id from edge_logs as el\nunion all\nselect pgl.id as id from postgres_logs as pgl\nunion all\nselect al.id as id from auth_logs as al
      """

      assert {:ok, _result} = Sql.transform(:ch_sql, input, user)
    end

    test "can extract out parameter names in the SQL string" do
      for {input, output} <- [
            {"select event_message, JSONExtractString(body, 'metadata.custom_user_data.company') AS company, timestamp FROM foo.ch WHERE company = @company",
             ["company"]},
            {"select @a from old", ["a"]}
          ] do
        assert {:ok, ^output} = Sql.parameters(input, dialect: "clickhouse")
      end
    end

    test "parameters positions are extracted" do
      ch_query =
        "select event_message, JSONExtractString(body, 'metadata.custom_user_data.company') AS company, timestamp FROM foo.ch WHERE company = @company"

      assert {:ok, %{1 => "company"}} = Sql.parameter_positions(ch_query, dialect: "clickhouse")
    end

    test "sandboxed queries work with simple CTEs" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"
      consumer_query = "select a from src where a > 5"

      assert {:ok, result} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(result) =~ "with src as"
      assert String.downcase(result) =~ "select a from src where a > 5"
    end

    test "sandboxed queries with order by" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"
      consumer_query = "select a from src order by a desc"

      assert {:ok, result} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(result) =~ "with src as"
      assert String.downcase(result) =~ "order by a desc"
    end

    test "sandboxed queries with union all" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = """
      with cte1 as (select a from my_ch_table),
           cte2 as (select b from my_ch_table)
      select a from cte1
      """

      consumer_query = "select a from cte1 union all select b from cte2"

      assert {:ok, result} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(result) =~ "union all"
      assert String.downcase(result) =~ "select a from cte1"
      assert String.downcase(result) =~ "select b from cte2"
    end

    test "sandboxed queries cannot access sources/tables outside of CTE scope" do
      user = insert(:user)
      source = insert(:source, user: user, name: "my_ch_table")
      other_source = insert(:source, user: user, name: "other_ch_table")

      # setup local clickhouse for e2e test
      {source, backend, cleanup_fn} = setup_clickhouse_test(source: source, user: user)
      on_exit(cleanup_fn)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})
      assert {:ok, _} = ClickhouseAdaptor.provision_ingest_table({source, backend})

      log_events = [
        build(:log_event,
          source: source,
          message: "Test message 1",
          metadata: %{"value" => 10}
        ),
        build(:log_event,
          source: source,
          message: "Test message 2",
          metadata: %{"value" => 20}
        )
      ]

      assert :ok = ClickhouseAdaptor.insert_log_events({source, backend}, log_events)

      Process.sleep(200)

      cte_query =
        "with src as (select body from #{source.name}) select body from src"

      consumer_query = "select body from src"

      assert {:ok, transformed} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert {:ok, results} = ClickhouseAdaptor.execute_query(backend, transformed, [])
      assert length(results) == 2

      # cannot access the source table directly
      consumer_query_accessing_source = "select body from #{source.name}"

      assert {:error, err} =
               Sql.transform(:ch_sql, {cte_query, consumer_query_accessing_source}, user)

      assert String.downcase(err) =~ "table not found in cte"

      # cannot access another known source that exists but is not in the CTE
      consumer_query_accessing_other_source = "select body from #{other_source.name}"

      assert {:error, err} =
               Sql.transform(:ch_sql, {cte_query, consumer_query_accessing_other_source}, user)

      assert String.downcase(err) =~ "table not found in cte"
    end

    test "sandboxed queries reject table references not in CTE" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"
      consumer_query = "select a from my_ch_table"

      assert {:error, err} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(err) =~ "table not found in cte"
    end

    test "sandboxed queries reject wildcards" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"
      consumer_query = "select * from src"

      assert {:error, err} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(err) =~ "restricted wildcard"
    end

    test "sandboxed queries reject DML operations" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"
      consumer_query = "delete from src where a = 1"

      assert {:error, err} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
      assert String.downcase(err) =~ "only select queries allowed"
    end

    test "rejects restricted functions" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      restricted_functions = [
        {"file", "select col1 from file('/etc/passwd', 'CSV')"},
        {"url", "select col1 from url('http://example.com/data.csv', 'CSV')"},
        {"s3", "select col1 from s3('s3://bucket/file.csv', 'CSV')"},
        {"remote", "select col1 from remote('localhost', 'default', 'table')"},
        {"mysql", "select col1 from mysql('localhost:3306', 'db', 'table', 'user', 'pass')"},
        {"currentuser", "select currentUser()"}
      ]

      for {function_name, query} <- restricted_functions do
        assert {:error, err} = Sql.transform(:ch_sql, query, user)
        assert String.downcase(err) =~ "restricted function #{function_name}"
      end
    end

    test "rejects restricted functions in sandboxed queries" do
      user = insert(:user)
      _source = insert(:source, user: user, name: "my_ch_table")

      cte_query = "with src as (select a from my_ch_table) select a from src"

      restricted_queries = [
        {"url", "select col1 from url('http://example.com/data.csv', 'CSV')"},
        {"s3", "select col1 from s3('s3://bucket/file.csv', 'CSV')"},
        {"currentuser", "select currentUser()"}
      ]

      for {function_name, consumer_query} <- restricted_queries do
        assert {:error, err} = Sql.transform(:ch_sql, {cte_query, consumer_query}, user)
        assert String.downcase(err) =~ "restricted function #{function_name}"
      end
    end
  end

  test "sources/2 creates a source mapping present for sources present in the query" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")
    other_source = insert(:source, user: user, name: "other.table")
    input = "select a from my_table"
    expected = %{"my_table" => Atom.to_string(source.token)}
    assert {:ok, ^expected} = Sql.sources(input, user)

    input = "select a from my_table, `other.table`"

    expected = %{
      "my_table" => Atom.to_string(source.token),
      "other.table" => Atom.to_string(other_source.token)
    }

    assert {:ok, ^expected} = Sql.sources(input, user)
  end

  test "sources/2 raises error on invalid query" do
    user = insert(:user)

    input = "select a from my_table, `other"
    assert {:error, "sql parser error" <> _} = Sql.sources(input, user)
  end

  test "source_mapping/3 updates an SQL string with renamed sources" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")

    mapping = %{
      "old" => Atom.to_string(source.token)
    }

    input = "select old.a from old"
    expected = "select new.a from new"

    Ecto.Changeset.change(source, name: "new")
    |> Logflare.Repo.update()

    assert {:ok, output} = Sql.source_mapping(input, user.id, mapping)
    assert String.downcase(output) == expected
    assert {:ok, output} = Sql.source_mapping(input, user, mapping)
    assert String.downcase(output) == expected
  end

  test "parameters/1 extracts out parameter names in the SQL string" do
    for {input, output} <- [
          {"select old.a from old where @test = 123", ["test"]},
          {"select @a from old", ["a"]},
          {"select old.a from old where char_length(@c)", ["c"]},
          # backticked function
          {"select `some.function`(@c)", ["c"]},
          # case statement in function arg
          {"select `some.function`(CASE WHEN @c = 'hourly' THEN 1 ELSE 1 END)", ["c"]},
          {"select `some.function`((CASE WHEN @c = 'hourly' THEN 1 ELSE 1 END))", ["c"]},
          # double function arg case statements with select alias
          {"select `some.function`((CASE WHEN @c = 'hourly' THEN 1 ELSE 1 END), (CASE WHEN @c = 'hourly' THEN 1 ELSE 1 END)) as d",
           ["c"]},
          # CTEs
          {"with q as (select old.a from old where char_length(@c)) select 1", ["c"]},
          {"with q as (select @c from old) select 1", ["c"]}
        ] do
      assert {:ok, ^output} = Sql.parameters(input)
      assert {:ok, ^output} = Sql.parameters(input)
    end
  end

  defp bq_table_name(
         %{user: user} = source,
         override_project_id \\ nil,
         override_dataset_id \\ nil
       ) do
    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    project_id = override_project_id || user.bigquery_project_id || @logflare_project_id
    dataset_id = override_dataset_id || user.bigquery_dataset_id || "#{user.id}_#{@env}"

    "`#{project_id}.#{dataset_id}.#{token}`"
  end

  test "expand_subqueries/2 for :bq_sql will expand an alert/endpoint query into a subquery" do
    alert = build(:alert, name: "my.alert", query: "select 'id' as id", language: :bq_sql)

    endpoint =
      build(:endpoint, name: "my.endpoint", query: "select 'val' as val", language: :bq_sql)

    assert {:ok, result} =
             Sql.expand_subqueries(:bq_sql, "select test from `my.alert` as tester", [alert])

    assert String.downcase(result) =~ "from (select 'id' as id) as tester"

    assert {:ok, result} =
             Sql.expand_subqueries(:bq_sql, "select test from `my.endpoint` as tester", [endpoint])

    assert String.downcase(result) =~ "from (select 'val' as val) as tester"
  end

  test "expand_subqueries/2 for :pg_sql will expand an alert/endpoint query into a subquery" do
    alert = build(:alert, name: "my.alert", query: "select 'id' as id", language: :pg_sql)

    endpoint =
      build(:endpoint, name: "my.endpoint", query: "select 'val' as val", language: :pg_sql)

    assert {:ok, result} =
             Sql.expand_subqueries(:pg_sql, ~s(select test from "my.alert" as tester), [alert])

    assert String.downcase(result) =~ "from (select 'id' as id) as tester"

    assert {:ok, result} =
             Sql.expand_subqueries(:pg_sql, ~s(select test from "my.endpoint" as tester), [
               endpoint
             ])

    assert String.downcase(result) =~ "from (select 'val' as val) as tester"
  end

  describe "transform/3 for :postgres backends" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, name: "source_a")
      %{user: user, source: source}
    end

    test "changes query on FROM command to correct table name", %{
      source: %{name: name} = source,
      user: user
    } do
      input = "SELECT body, event_message, timestamp FROM #{name}"

      assert {:ok, transformed} = Sql.transform(:pg_sql, input, user)
      assert transformed =~ ~s("#{PostgresAdaptor.table_name(source)}")
    end
  end

  describe "contains_cte?/2" do
    test "returns true for queries with CTEs" do
      query_with_cte = """
      WITH users_summary AS (
        SELECT user_id, COUNT(*) as total_events
        FROM events
        GROUP BY user_id
      )
      SELECT * FROM users_summary WHERE total_events > 10
      """

      assert Sql.contains_cte?(query_with_cte)

      query_with_multiple_ctes = """
      WITH
        users_summary AS (
          SELECT user_id, COUNT(*) as total_events
          FROM events
          GROUP BY user_id
        ),
        recent_events AS (
          SELECT * FROM events WHERE timestamp > '2023-01-01'
        )
      SELECT u.user_id, u.total_events, r.timestamp
      FROM users_summary u
      JOIN recent_events r ON u.user_id = r.user_id
      """

      assert Sql.contains_cte?(query_with_multiple_ctes)

      recursive_cte = """
      WITH RECURSIVE employee_hierarchy AS (
        SELECT employee_id, manager_id, name, 0 as level
        FROM employees
        WHERE manager_id IS NULL
        UNION ALL
        SELECT e.employee_id, e.manager_id, e.name, eh.level + 1
        FROM employees e
        JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
      )
      SELECT * FROM employee_hierarchy
      """

      assert Sql.contains_cte?(recursive_cte)
    end

    test "returns false for queries without CTEs" do
      simple_query = "SELECT * FROM users"
      refute Sql.contains_cte?(simple_query)

      join_query = """
      SELECT u.name, e.event_type
      FROM users u
      JOIN events e ON u.id = e.user_id
      WHERE u.active = true
      """

      refute Sql.contains_cte?(join_query)

      subquery = """
      SELECT *
      FROM users
      WHERE id IN (SELECT user_id FROM events WHERE event_type = 'login')
      """

      refute Sql.contains_cte?(subquery)

      complex_query = """
      SELECT
        u.name,
        COUNT(e.id) as event_count,
        AVG(e.duration) as avg_duration
      FROM users u
      LEFT JOIN events e ON u.id = e.user_id
      WHERE u.created_at > '2023-01-01'
      GROUP BY u.id, u.name
      HAVING COUNT(e.id) > 5
      ORDER BY event_count DESC
      LIMIT 10
      """

      refute Sql.contains_cte?(complex_query)
    end

    test "works with different SQL dialects" do
      cte_query = """
      WITH user_stats AS (
        SELECT user_id, COUNT(*) as count
        FROM events
        GROUP BY user_id
      )
      SELECT * FROM user_stats
      """

      assert Sql.contains_cte?(cte_query)
      assert Sql.contains_cte?(cte_query, dialect: "bigquery")
      assert Sql.contains_cte?(cte_query, dialect: "postgres")
    end

    test "case insensitive WITH detection" do
      assert Sql.contains_cte?("with cte as (select 1) select * from cte")
      assert Sql.contains_cte?("WITH CTE AS (SELECT 1) SELECT * FROM CTE")
      assert Sql.contains_cte?("With Cte As (Select 1) Select * From Cte")
    end
  end

  describe "translate/2 with nested fields" do
    setup [:setup_postgres_backend]

    test "translate operator to numeric with between with 1 level nested field reference",
         %{user: user, source: source, backend: backend} = ctx do
      insert_log_event(ctx, %{
        "event_message" => "something",
        "col" => %{"nested" => 223}
      })

      insert_log_event(ctx, %{
        "event_message" => "something",
        "col" => %{"nested" => 400}
      })

      bq_query = ~s"""
      select count(t.id) as count from `#{source.name}` t
      cross join unnest(t.col) as c
      where c.nested between 200 and 299
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)
      assert {:ok, [%{"count" => 1}]} = PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "translate operator to numeric with between with 2 level nested field reference",
         %{user: user, source: source, backend: backend} = ctx do
      insert_log_event(ctx, %{
        "event_message" => "something",
        "col" => %{"nested" => %{"num" => 223}}
      })

      insert_log_event(ctx, %{
        "event_message" => "something",
        "col" => %{"nested" => %{"num" => 400}}
      })

      bq_query = ~s"""
      select count(t.id) as count from `#{source.name}` t
      cross join unnest(t.col) as c
      cross join unnest(c.nested) as d
      where d.num between 200 and 299
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)
      assert {:ok, [%{"count" => 1}]} = PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "CTE translation with cross join",
         %{user: user, source: source, backend: backend} = ctx do
      insert_log_event(ctx, %{
        "event_message" => "something",
        "metadata" => %{
          "request" => %{"method" => "GET", "path" => "/"},
          "response" => %{"status_code" => 200}
        }
      })

      bq_query = ~s"""
      select count(CASE WHEN (req.method IN ('GET', 'POST')) THEN 1 END) as count,
      from  `#{source.name}` t
      cross join unnest(metadata) as m
      cross join unnest(m.request) as req
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok,
              [
                %{
                  "count" => 1
                }
              ]} = PostgresAdaptor.execute_query(backend, transformed, [])
    end
  end

  describe "translate/2 with CTEs" do
    setup [:setup_postgres_backend]

    test "CTE translation with cross join",
         %{user: user, source: source, backend: backend} = ctx do
      insert_log_event(ctx, %{
        "event_message" => "something",
        "metadata" => %{
          "request" => %{"method" => "GET", "path" => "/"},
          "response" => %{"status_code" => 200}
        }
      })

      bq_query = ~s"""
      with logs as (
        select t.timestamp, t.id, t.event_message, t.metadata
        from  `#{source.name}` t
        cross join unnest(metadata) as m
      )
      select event_message, request.method, request.path, response.status_code
      from logs
      cross join unnest(metadata) as m
      cross join unnest(m.request) as request
      cross join unnest(m.response) as response
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok,
              [
                %{
                  "event_message" => "something",
                  "method" => "GET",
                  "path" => "/",
                  "status_code" => 200
                }
              ]} = PostgresAdaptor.execute_query(backend, transformed, [])
    end
  end

  describe "bq -> pg translation" do
    setup [:setup_postgres_backend]

    setup %{source: source, backend: backend} do
      log_event =
        Logflare.LogEvent.make(
          %{
            "event_message" => "something",
            "test" => "data",
            "metadata" => %{"nested" => "value", "num" => 123}
          },
          %{source: source}
        )

      PostgresAdaptor.insert_log_event(source, backend, log_event)
      :ok
    end

    test "UNNESTs into JSON-Query", %{backend: backend, user: user} do
      bq_query = """
      select test, m.nested from `c.d.e` t
      cross join unnest(t.metadata) as m
      where m.nested is not null
      """

      assert {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)

      translated = String.downcase(translated)
      # changes source quotes
      assert translated =~ ~s("c.d.e")
      assert translated =~ "body -> 'test'"
      assert translated =~ "body #> '{metadata,nested}'"
      # remove cross joining
      refute translated =~ "cross join"
      refute translated =~ "unnest"

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)
      # execute it on PG
      assert {:ok, [%{"test" => "data", "nested" => "value"}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "translate operator to numeric when nested field reference present", %{
      backend: backend,
      user: user
    } do
      bq_query = ~s"""
      select count(t.id) as count  from `c.d.e` t
      cross join unnest(t.metadata) as m
      where m.num > 100
      """

      pg_query = ~s"""
      select count((t.body -> 'id')) as count  from "c.d.e" t
      where ((body #>> '{metadata,num}')::jsonb #>> '{}')::numeric > 100
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"count" => 1}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "REGEXP_CONTAINS is translated", %{backend: backend, user: user} do
      bq_query = ~s|select regexp_contains("string", "str") as has_substring|

      pg_query = ~s|select 'string' ~ 'str' as has_substring|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"has_substring" => true}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "REGEXP_CONTAINS is translated with field reference", %{backend: backend, user: user} do
      bq_query = ~s|select regexp_contains(t.test, "str") as has_substring from `c.d.e` t|

      pg_query = ~s|select (t.body ->> 'test') ~ 'str' as has_substring from "c.d.e" t|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"has_substring" => false}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "REGEXP_CONTAINS is translated with field reference with nested field", %{
      backend: backend,
      user: user
    } do
      bq_query =
        ~s|select regexp_contains(m.nested, "val") as has_substring from `c.d.e` t cross join unnest(t.metadata) as m|

      pg_query = ~s|select (body #>> '{metadata,nested}') ~ 'val' as has_substring from "c.d.e" t|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"has_substring" => true}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "REGEXP_CONTAINS is translated with field reference with nested field in where", %{
      backend: backend,
      user: user
    } do
      bq_query =
        ~s|select m.nested as nested from `c.d.e` t cross join unnest(t.metadata) as m where regexp_contains(m.nested, "val")|

      pg_query =
        ~s|select (body #> '{metadata,nested}') as nested from "c.d.e" t where (body #>> '{metadata,nested}') ~ 'val'|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"nested" => "value"}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "REGEXP_CONTAINS is translated with field reference with nested field in where with cte",
         %{backend: backend, user: user, source: source} do
      # Insert test data with the required nested structure
      log_event =
        Logflare.LogEvent.make(
          %{
            "event_message" => "something",
            "test" => "data",
            "metadata" => %{"nested" => "value", "num" => 123, "deep" => %{"even" => "deeper"}}
          },
          %{source: source}
        )

      PostgresAdaptor.insert_log_event(source, backend, log_event)

      bq_query =
        ~s|with data as (select metadata from `c.d.e`) select d.even as nested from data t cross join unnest(t.deep) as d where regexp_contains(d.even, "deep")|

      pg_query =
        ~s|with data as (select (body -> 'metadata') as metadata from "c.d.e") select (t.metadata #>> '{deep,even}') as nested from data t where (t.metadata #>> '{deep,even}') ~ 'deep'|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      assert {:ok, transformed} = Sql.transform(:pg_sql, translated, user)

      assert {:ok, [%{"nested" => "deeper"}]} =
               PostgresAdaptor.execute_query(backend, transformed, [])
    end

    test "entities backtick to double quote" do
      bq_query = """
      select test from `c.d.e`
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert translated =~ ~s("c.d.e")
    end

    test "countif into count-filter" do
      bq_query = "select countif(test = '1') from my_table"
      pg_query = ~s|select count(*) filter (where (body ->> 'test') = '1') from my_table|
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "current_timestamp handling " do
      bq_query = "select current_timestamp() as t"
      pg_query = ~s|select current_timestamp as t|
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
      refute translated =~ "current_timestamp()"

      # in cte
      bq_query = "with a as (select current_timestamp() as t) select a.t"
      pg_query = ~s|with a as (select current_timestamp as t) select a.t as t|
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
      refute translated =~ "current_timestamp()"
    end

    test "timestamp_sub" do
      bq_query = "select timestamp_sub(current_timestamp(), interval 1 day) as t"
      pg_query = ~s|select current_timestamp - interval '1 day' as t|
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "timestamp_trunc without a field reference" do
      bq_query = "select timestamp_trunc(current_timestamp(), day) as t"
      pg_query = ~s|select date_trunc('day', current_timestamp) as t|
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE aliases are not converted to json query" do
      bq_query =
        "with test as (select id, metadata from mytable) select id, metadata.request from test"

      pg_query =
        ~s|with test as (select (body -> 'id') as id, (body -> 'metadata') as metadata from mytable) select id as id, (metadata -> 'request') as request from test|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE alias fields do not get converted to json query if referenced" do
      bq_query = ~s"""
      with a as (
        select 'test' as col
      ),
      b as (
        select 'btest' as other from a, my_table t
        where a.col = t.my_col
      )
      select a.col from a
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
      ),
      b as (
        select 'btest' as other from a, my_table t
        where (a.col::jsonb #>> '{}' )  = (t.body ->> 'my_col')
      )
      select a.col as col from a
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE table quotations are converted" do
      bq_query = ~s"""
      with a as (select 'test' from `my.table` t) select 'test' from `a`
      """

      pg_query = ~s"""
      with a as (
        select 'test'
        from "my.table" t
      ) select 'test' from "a"
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE cross join UNNESTs are removed" do
      bq_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        cross join unnest(t.metadata) as m
      ) select a.col from a
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
      ) select a.col as col from a
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE order by is " do
      bq_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast(t.my_col as timestamp) desc
      ) select a.col from a
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast( (t.body ->> 'my_col') as timestamp) desc
        ) select a.col as col from a
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE order by without from " do
      bq_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast(t.my_col as timestamp) desc
      ) select 'tester' as col
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast( (t.body ->> 'my_col') as timestamp) desc
        ) select 'tester' as col
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE cross join UNNESTs with filter reference" do
      bq_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        cross join unnest(t.metadata) as m
        where m.project = '123'
      ) select a.col from a
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        where (body #>> '{metadata,project}') = '123'
        ) select a.col as col from a
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE cross join UNNESTs with multiple from" do
      bq_query = ~s"""
      with c as (select '123' as val), a as (
        select 'test' as col
        from c, my_table t
        cross join unnest(t.metadata) as m
        where m.project = '123'
      ) select a.col from a
      """

      pg_query = ~s"""
      with c as (select '123' as val), a as (
        select 'test' as col
        from c, my_table t
        where (body #>> '{metadata,project}') = '123'
        ) select a.col as col from a
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "field references within a cast() are converted to ->> syntax for string casting" do
      bq_query = ~s|select cast(col as timestamp) as date from my_table|
      pg_query = ~s|select cast( (body ->> 'col') as timestamp) as date from my_table|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "field references within a DATE_TRUNC() are converted to ->> syntax for string casting" do
      bq_query = ~s|select DATE_TRUNC('day', col) as date from my_table|
      pg_query = ~s|select DATE_TRUNC('day',  (body ->> 'col')) as date from my_table|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "field references in left-right operators are converted to ->> syntax" do
      bq_query = ~s|select t.id = 'test' as value from my_table t|
      pg_query = ~s|select (t.body ->> 'id') = 'test' as value from my_table t|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "order by json query" do
      bq_query = ~s|select id from my_source t order by t.my_col|

      pg_query = ~s|select (body -> 'id') as id from my_source t order by (t.body -> 'my_col')|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    # test "cte WHERE identifiers are translated correctly"

    test "parameters are translated" do
      # test that substring of another arg is replaced correctly
      bq_query =
        ~s|select @test as arg1, @test_another as arg2, coalesce(@test, '') > @test as arg_copy|

      pg_query =
        ~s|select $1::text as arg1, $2::text as arg2, coalesce($3::text, '') > $4::text as arg_copy|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
      # determines sequence of parameters
      assert {:ok, %{1 => "test", 2 => "test_another", 4 => "test"}} =
               Sql.parameter_positions(bq_query)
    end

    test "malformed table name when global bq project id is not set" do
      # if global bq project id is not set, the first part will be empty
      input =
        "SELECT body, event_message, timestamp FROM `.1_prod.b658a216_0aef_427e_bae8_9dfc68aad6dd`"

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, input)
      assert translated =~ ~s("log_events_b658a216_0aef_427e_bae8_9dfc68aad6dd")
    end

    test "custom schema prefixing" do
      input =
        "SELECT body, event_message, timestamp FROM `.1_prod.b658a216_0aef_427e_bae8_9dfc68aad6dd`"

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, input)
      assert translated =~ ~s("log_events_b658a216_0aef_427e_bae8_9dfc68aad6dd")
      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, input, "my_schema")
      assert translated =~ ~s("my_schema"."log_events_b658a216_0aef_427e_bae8_9dfc68aad6dd")
    end

    test "unix microsecond timestamp handling" do
      bq_query = ~s|select t.timestamp as ts from my_table t|

      pg_query = ~s|select (t.body -> 'timestamp') as ts from my_table t|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)

      # only convert if not in projection
      bq_query = ~s|select t.id as id from my_table t where t.timestamp is not null|

      pg_query =
        ~s|select (t.body -> 'id') as id from my_table t where (to_timestamp( (t.body ->> 'timestamp')::bigint / 1000000.0) AT TIME ZONE 'UTC') is not null|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "special handling of timestamp field and date_trunc : " do
      bq_query = ~s"""
      with edge_logs as (select t.timestamp from  `cloudflare.logs.prod` t)
      select timestamp_trunc(t.timestamp, day) as timestamp from edge_logs t
      """

      pg_query = ~s"""
      with edge_logs as ( select (t.body -> 'timestamp') as timestamp from  "cloudflare.logs.prod" t )
      SELECT date_trunc('day', (to_timestamp( t.timestamp::bigint / 1000000.0) AT TIME ZONE 'UTC') ) AS timestamp FROM edge_logs t
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "special handling of timestamp field for binary ops" do
      bq_query = ~s"""
      with edge_logs as (select t.timestamp from  `cloudflare.logs.prod` t)
      select t.timestamp as timestamp from edge_logs t
      where t.timestamp > '2023-08-05T09:00:00.000Z'
      """

      pg_query = ~s"""
      with edge_logs as ( select (t.body -> 'timestamp') as timestamp from  "cloudflare.logs.prod" t )
      SELECT t.timestamp AS timestamp FROM edge_logs t
      where (to_timestamp(CAST(t.timestamp::TEXT AS BIGINT) / 1000000.0) AT TIME ZONE 'UTC') > '2023-08-05T09:00:00.000Z'
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "CTE fields in binary op are cast to text only when equal" do
      bq_query = ~s"""
      with edge_logs as (select t.id from  `cloudflare.logs.prod` t)
      select t.id as id from edge_logs t
      where t.id = '123'
      """

      pg_query = ~s"""
      with edge_logs as ( select (t.body -> 'id') as id from  "cloudflare.logs.prod" t )
      SELECT t.id AS id FROM edge_logs t
      where (cast(t.id as jsonb) #>> '{}') = '123'
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "translate in operator arguments to text" do
      bq_query = ~s"""
      select t.col as col from `my.source` t
      where t.col in ('val') and t.col not in ('other')
      """

      pg_query = ~s"""
      select (t.body -> 'col') as col from "my.source" t
      where (t.body ->> 'col') in ('val') and (t.body ->> 'col') not in ('other')
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "translate between operator sides to numeric" do
      bq_query = ~s"""
      select t.col as col from `my.source` t
      where t.col between 200 and 299
      """

      pg_query = ~s"""
      select (t.body -> 'col') as col from "my.source" t
      where (t.body ->> 'col')::numeric  between 200 and 299
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "translate >, >=, =, <, <=  operator to numeric if comparison side is a number" do
      bq_query = ~s"""
      with mytable as (select f.col as col from `my.source` f)
      select t.col as col from mytable t
      where t.col >= 123
        and t.col <= 123
        and t.col = 123
        and t.col > 123
        and t.col < 123
        and 123 >= t.col
        and 123 <= t.col
        and 123 = t.col
        and 123 > t.col
        and 123 < t.col
      """

      pg_query = ~s"""
      with mytable as (select (f.body -> 'col') as col from "my.source" f)
      select t.col as col from mytable t
      where (t.col::jsonb #>> '{}')::numeric  >= 123
        and (t.col::jsonb #>> '{}')::numeric  <= 123
        and (t.col::jsonb #>> '{}')::numeric  = 123
        and (t.col::jsonb #>> '{}')::numeric  > 123
        and (t.col::jsonb #>> '{}')::numeric  < 123
        and 123 >= (t.col::jsonb #>> '{}')::numeric
        and 123 <= (t.col::jsonb #>> '{}')::numeric
        and 123 = (t.col::jsonb #>> '{}')::numeric
        and 123 > (t.col::jsonb #>> '{}')::numeric
        and 123 < (t.col::jsonb #>> '{}')::numeric
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    # functions metrics
    # test "APPROX_QUANTILES is translated"
    # tes "offset() and indexing is translated"
  end

  defp setup_postgres_backend(_context) do
    repo = Application.get_env(:logflare, Logflare.Repo)

    config = %{
      url:
        "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"
    }

    user = insert(:user)
    source = insert(:source, user: user, name: "c.d.e")
    backend = insert(:backend, type: :postgres, sources: [source], config: config)

    pid = start_supervised!({AdaptorSupervisor, {source, backend}})

    on_exit(fn ->
      PostgresAdaptor.destroy_instance({source, backend})
    end)

    %{source: source, backend: backend, pid: pid, user: user}
  end

  defp insert_log_event(%{source: source, backend: backend}, event) do
    log_event =
      Logflare.LogEvent.make(
        event,
        %{source: source}
      )

    PostgresAdaptor.insert_log_event(source, backend, log_event)
  end
end
