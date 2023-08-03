defmodule Logflare.SqlTest do
  @moduledoc false
  use Logflare.DataCase, async: false
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  @logflare_project_id "logflare-project-id"
  @user_project_id "user-project-id"
  @user_dataset_id "user-dataset-id"
  @env "test"

  setup do
    values = Application.get_env(:logflare, Logflare.Google)
    to_put = Keyword.put(values, :project_id, @logflare_project_id)
    Application.put_env(:logflare, Logflare.Google, to_put)

    on_exit(fn ->
      Application.put_env(:logflare, Logflare.Google, values)
    end)
  end

  describe "bigquery dialect" do
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
            # no source name in query
            {"select datetime() from light-two-os-directions-test",
             "can't find source light-two-os-directions-test"},
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
  end

  describe "Bigquery fully qualified" do
    test "able to use fully qualified names in queries" do
      user =
        insert(:user, bigquery_project_id: @user_project_id, bigquery_dataset_id: @user_dataset_id)

      source_abc = insert(:source, user: user, name: "a.b.c")
      source_cxy = insert(:source, user: user, name: "c.x.y")
      source_cxyz = insert(:source, user: user, name: "c.x.y.z")

      for {input, expected} <- [
            # fully qualified names must start with the user's bigquery project
            {"select a from `#{@user_project_id}.#{@user_dataset_id}.mytable`",
             "select a from `#{@user_project_id}.#{@user_dataset_id}.mytable`"},
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

    # This test checks if a source name starting with a logflare project id will get transformed correctly to the source value
    # this ensures  users cannot access other users' sources.
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
      assert transformed =~ "#{@single_tenant_bq_project_id}.my_dataset.my_table"
      refute transformed =~ @logflare_project_id
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

  describe "bq -> pg translation" do
    setup do
      repo = Application.get_env(:logflare, Logflare.Repo)

      config = %{
        "url" =>
          "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"
      }

      user = insert(:user)
      source = insert(:source, user: user, name: "c.d.e")
      source_backend = insert(:source_backend, type: :postgres, source: source, config: config)

      pid = start_supervised!({PostgresAdaptor, source_backend})

      log_event =
        Logflare.LogEvent.make(
          %{
            "event_message" => "something",
            "test" => "data",
            "metadata" => %{"nested" => "value"}
          },
          %{source: source}
        )

      PostgresAdaptor.insert_log_event(source_backend, log_event)

      on_exit(fn ->
        PostgresAdaptor.rollback_migrations(source_backend)
        PostgresAdaptor.drop_migrations_table(source_backend)
      end)

      %{source: source, source_backend: source_backend, pid: pid, user: user}
    end

    test "UNNESTs into JSON-Query", %{source_backend: source_backend, user: user} do
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
               PostgresAdaptor.execute_query(source_backend, transformed)
    end

    test "entities backtick to double quote" do
      bq_query = """
      select test from `c.d.e`
      """

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert translated =~ ~s("c.d.e")
    end

    test "countif into count-filter" do
      bq_query = "select countif(test = 1) from my_table"
      pg_query = ~s|select count(*) filter (where (body ->> 'test') = 1) from my_table|
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

    test "timestamp_trunc" do
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
        where a.col = (t.body ->> 'my_col')
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
        order by cast(t.timestamp as timestamp) desc
      ) select a.col from a
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast( (t.body ->> 'timestamp') as timestamp) desc
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
        order by cast(t.timestamp as timestamp) desc
      ) select 'tester' as col
      """

      pg_query = ~s"""
      with a as (
        select 'test' as col
        from my_table t
        order by cast( (t.body ->> 'timestamp') as timestamp) desc
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

    test "field references in left-right operators are converted to ->> syntax" do
      bq_query = ~s|select t.id = 'test' as value from my_table t|
      pg_query = ~s|select (t.body ->> 'id') = 'test' as value from my_table t|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
    end

    test "order by json query" do
      bq_query = ~s|select id from my_source t order by t.timestamp|

      pg_query = ~s|select (body -> 'id') as id from my_source t order by (t.body -> 'timestamp')|

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

    test "REGEXP_CONTAINS is translated" do
      bq_query = ~s|select regexp_contains("string", "str") as has_substring|

      pg_query = ~s|select 'string' ~ 'str' as has_substring|

      {:ok, translated} = Sql.translate(:bq_sql, :pg_sql, bq_query)
      assert Sql.Parser.parse("postgres", translated) == Sql.Parser.parse("postgres", pg_query)
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

    # functions metrics
    # test "APPROX_QUANTILES is translated"
    # tes "offset() and indexing is translated"
  end
end
