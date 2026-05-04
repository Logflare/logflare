defmodule Logflare.Sql.AstUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Sql.AstUtils
  doctest AstUtils

  describe "transform_recursive/3" do
    test "applies transformation to matching nodes" do
      ast = {"Table", %{"name" => "users"}}

      result =
        AstUtils.transform_recursive(ast, nil, fn
          {"Table", %{"name" => name}}, _data ->
            {"Table", %{"name" => String.upcase(name)}}

          node, _data ->
            {:recurse, node}
        end)

      assert result == {"Table", %{"name" => "USERS"}}
    end

    test "recursively traverses nested structures" do
      ast = %{
        "Query" => %{
          "from" => [
            {"Table", %{"name" => "users"}},
            {"Table", %{"name" => "posts"}}
          ]
        }
      }

      result =
        AstUtils.transform_recursive(ast, nil, fn
          {"Table", %{"name" => name}}, _data ->
            {"Table", %{"name" => "transformed_#{name}"}}

          node, _data ->
            {:recurse, node}
        end)

      expected = %{
        "Query" => %{
          "from" => [
            {"Table", %{"name" => "transformed_users"}},
            {"Table", %{"name" => "transformed_posts"}}
          ]
        }
      }

      assert result == expected
    end

    test "handles lists correctly" do
      ast = [
        {"Table", %{"name" => "users"}},
        {"Function", %{"name" => "count"}},
        {"Table", %{"name" => "posts"}}
      ]

      result =
        AstUtils.transform_recursive(ast, nil, fn
          {"Table", %{"name" => name}}, _data ->
            {"Table", %{"name" => "table_#{name}"}}

          node, _data ->
            {:recurse, node}
        end)

      expected = [
        {"Table", %{"name" => "table_users"}},
        {"Function", %{"name" => "count"}},
        {"Table", %{"name" => "table_posts"}}
      ]

      assert result == expected
    end

    test "passes data through transformation" do
      ast = {"Table", %{"name" => "users"}}
      data = %{prefix: "test_"}

      result =
        AstUtils.transform_recursive(ast, data, fn
          {"Table", %{"name" => name}}, %{prefix: prefix} ->
            {"Table", %{"name" => prefix <> name}}

          node, _data ->
            {:recurse, node}
        end)

      assert result == {"Table", %{"name" => "test_users"}}
    end

    test "stops recursion when transformation doesn't return {:recurse, node}" do
      ast = %{
        "Query" => %{
          "from" => {"Table", %{"name" => "users"}}
        }
      }

      result =
        AstUtils.transform_recursive(ast, nil, fn
          %{"Query" => _query}, _data ->
            # Do not recurse into Query nodes
            %{"Query" => "REPLACED"}

          node, _data ->
            {:recurse, node}
        end)

      assert result == %{"Query" => "REPLACED"}
    end
  end

  describe "collect_from_ast/2" do
    test "collects matching items from simple structure" do
      ast = [
        {"Placeholder", "@param1"},
        {"Function", %{"name" => "count"}},
        {"Placeholder", "@param2"}
      ]

      result =
        AstUtils.collect_from_ast(ast, fn
          {"Placeholder", "@" <> param} -> {:collect, param}
          _node -> :skip
        end)

      assert result == ["param1", "param2"]
    end

    test "handles empty results" do
      ast = {"Function", %{"name" => "count"}}

      result =
        AstUtils.collect_from_ast(ast, fn
          {"Placeholder", _} -> {:collect, "found"}
          _node -> :skip
        end)

      assert result == []
    end

    test "collects multiple items from same node type" do
      ast = [
        {"Table", %{"name" => "users"}},
        {"Table", %{"name" => "posts"}},
        {"Function", %{"name" => "count"}},
        {"Table", %{"name" => "comments"}}
      ]

      result =
        AstUtils.collect_from_ast(ast, fn
          {"Table", %{"name" => name}} -> {:collect, name}
          _node -> :skip
        end)

      assert result == ["users", "posts", "comments"]
    end

    test "preserves order of collection" do
      ast = [
        {"Item", "first"},
        {"Item", "second"},
        {"Item", "third"}
      ]

      result =
        AstUtils.collect_from_ast(ast, fn
          {"Item", value} -> {:collect, value}
          _node -> :skip
        end)

      assert result == ["first", "second", "third"]
    end
  end

  describe "practical usage patterns" do
    test "demonstrates table name transformation pattern" do
      # Shows how to transform table names in SQL ASTs
      ast = [
        {"Table", %{"name" => "users"}},
        {"Table", %{"name" => "posts"}}
      ]

      source_mapping = %{"users" => "src_123", "posts" => "src_456"}

      result =
        AstUtils.transform_recursive(ast, source_mapping, fn
          {"Table", %{"name" => name}}, mapping ->
            case Map.get(mapping, name) do
              nil -> {"Table", %{"name" => name}}
              source_id -> {"Table", %{"name" => "table_#{source_id}"}}
            end

          node, _data ->
            {:recurse, node}
        end)

      expected = [
        {"Table", %{"name" => "table_src_123"}},
        {"Table", %{"name" => "table_src_456"}}
      ]

      assert result == expected
    end

    test "demonstrates parameter extraction pattern" do
      ast = [
        {"Placeholder", "@user_id"},
        {"Function", %{"name" => "count"}},
        {"Placeholder", "@status"}
      ]

      parameters =
        AstUtils.collect_from_ast(ast, fn
          {"Placeholder", "@" <> param} -> {:collect, param}
          _node -> :skip
        end)

      assert Enum.sort(parameters) == ["status", "user_id"]
    end

    test "demonstrates function validation pattern" do
      ast = [
        {"Function", %{"name" => "count"}},
        # restricted
        {"Function", %{"name" => "session_user"}},
        {"Function", %{"name" => "sum"}}
      ]

      restricted_functions =
        AstUtils.collect_from_ast(ast, fn
          {"Function", %{"name" => name}} ->
            if name in ["session_user", "external_query"] do
              {:collect, name}
            else
              :skip
            end

          _node ->
            :skip
        end)

      assert restricted_functions == ["session_user"]
    end
  end

  describe "edge cases" do
    test "handles nil and empty structures" do
      assert AstUtils.transform_recursive(nil, nil, fn x, _ -> x end) == nil
      assert AstUtils.transform_recursive([], nil, fn x, _ -> {:recurse, x} end) == []
      assert AstUtils.transform_recursive(%{}, nil, fn x, _ -> {:recurse, x} end) == %{}
    end

    test "handles primitive values" do
      result =
        AstUtils.transform_recursive("string", nil, fn
          "string", _ -> "transformed"
          x, _ -> {:recurse, x}
        end)

      assert result == "transformed"
    end

    test "collect handles empty structures" do
      assert AstUtils.collect_from_ast(nil, fn _ -> :skip end) == []
      assert AstUtils.collect_from_ast([], fn _ -> :skip end) == []
      assert AstUtils.collect_from_ast(%{}, fn _ -> :skip end) == []
    end
  end
end
