alias Logflare.Rules.Rule
alias Logflare.Lql.Rules.FilterRule

# alias Logflare.Lql.Parser
# IO.inspect(Parser.parse("~(?i)server\_\d"))
# IO.inspect(Parser.parse(~s|-m.user.company:"My Company"|))

lql_string = "metadata.field1:0 metadata.field2:~string"
byte_size(lql_string) |> IO.inspect(label: "lql byte size")

byte_size(:zlib.gzip(lql_string))
|> IO.inspect(label: "serialized lql byte size")

inspect_size = fn term, label ->
  size = :erts_debug.size(term) |> IO.inspect(label: "[#{label}] size")
  flat_size = :erts_debug.flat_size(term)

  if flat_size != size do
    IO.inspect(flat_size, label: "[#{label}] flat_size")
  end

  serialized = :erlang.term_to_binary(term)

  byte_size(serialized)
  |> IO.inspect(label: "[#{label}] serialized term size")

  byte_size(:zlib.gzip(serialized))
  |> IO.inspect(label: "[#{label}] gzipped serialized term size")
end

##### Rule set
rule_set =
  %{
    {:get, "metadata"} => %{
      {:get, "field1"} => %{
        {:equal, 0} => {:route, 1}
      },
      {:get, "field2"} => %{
        {:match, "string"} => {:route, 1}
      }
    }
  }

inspect_size.(rule_set, "rule_set")

#######
# Using a struct blows the serialized term
rule =
  %Rule{
    id: 1,
    lql_string: "",
    lql_filters: [
      %FilterRule{
        value: 0,
        operator: :=,
        modifiers: %{},
        path: "metadata.field1"
      },
      %FilterRule{
        value: "string",
        operator: :"~",
        modifiers: %{},
        path: "metadata.field2"
      }
    ]
  }

inspect_size.(rule, "struct")

# rule = %Rule{
rule = %{
  id: 1,
  lql_string: "",
  lql_filters: [
    %FilterRule{
      value: 0,
      operator: :=,
      modifiers: %{},
      path: "metadata.field1"
    },
    %FilterRule{
      value: "string",
      operator: :"~",
      modifiers: %{},
      path: "metadata.field2"
    }
  ]
}

inspect_size.(rule, "map from struct")

rule = %{
  rule
  | lql_filters: [
      {FilterRule, 0, :=, nil, nil, "metadata.field1"},
      {FilterRule, "string", :"~", nil, nil, "metadata.field2"}
    ]
}

inspect_size.(rule, "record")

rule = %{
  rule
  | lql_filters: [
      {FilterRule, 0, :=, nil, nil, ["metadata", "field1"]},
      {FilterRule, "string", :"~", nil, nil, ["metadata", "field2"]}
    ]
}

inspect_size.(rule, "record - path list")

rule = %{
  rule
  | lql_filters: [
      {"metadata", [{"field1", [{:=, 0, nil, nil}]}, {"field2", [{:"~", "string", nil, nil}]}]}
    ]
}

inspect_size.(rule, "Tree tuples")

fa = fn
  %{"metadata" => %{"field1" => 0}} -> true
  _ -> false
end

fb = fn %{"metadata" => %{"field2" => string}} -> String.match?(string, "string") end

rule = %{rule | lql_filters: [fa, fb]}
# NOTE: Lambdas are reported as super small, but in reality only the reference is small.
# The function code resides elswhere and is not included.
# Also, the serialization is problematic, as the function must exist on a node where it is deserialized
# Otherwise it will fail to load. This means if lambda is created dynamically serialization is a no-go
inspect_size.(rule, "Lambdas")

fa =
  quote do
    fn
      %{"metadata" => %{"field1" => 0}} -> true
      _ -> false
    end
  end
  |> tap(&IO.inspect(byte_size(:erlang.term_to_binary(&1)), label: "AST1"))
  |> Code.eval_quoted()

fb =
  quote do
    fn %{"metadata" => %{"field2" => string}} -> String.match?(string, "string") end
  end
  |> tap(&IO.inspect(byte_size(:erlang.term_to_binary(&1)), label: "AST2"))
  |> Code.eval_quoted()

rule = %{rule | lql_filters: [fa, fb]}

inspect_size.(rule, "quoted lambdas")

{filter_fun, _} =
  quote do
    fa =
      fn
        %{"metadata" => %{"field1" => 0}} -> true
        _ -> false
      end

    fb =
      fn %{"metadata" => %{"field2" => string}} -> String.match?(string, "string") end

    fn le -> fa.(le) and fb.(le) end
  end
  |> tap(&IO.inspect(byte_size(:erlang.term_to_binary(&1)), label: "ASTcombined"))
  |> Code.eval_quoted(prune_binding: true)

rule = %{rule | lql_string: "", lql_filters: filter_fun}

inspect_size.(rule, "combined quoted lambdas")

# lql byte size: 41
# serialized lql byte size: 50
# [rule_set] size: 59
# [rule_set] serialized term size: 130
# [rule_set] gzipped serialized term size: 95
# [struct] size: 132
# [struct] serialized term size: 917
# [struct] gzipped serialized term size: 380
# [map from struct] size: 69
# [map from struct] serialized term size: 353
# [map from struct] gzipped serialized term size: 194
# [record] size: 41
# [record] serialized term size: 207
# [record] gzipped serialized term size: 142
# [record - path list] size: 53
# [record - path list] serialized term size: 227
# [record - path list] gzipped serialized term size: 147
# [Tree tuples] size: 53
# [Tree tuples] serialized term size: 150
# [Tree tuples] gzipped serialized term size: 116
# [Lambdas] size: 28
# [Lambdas] serialized term size: 218
# [Lambdas] gzipped serialized term size: 142
# AST1: 137
# AST2: 218
# [quoted lambdas] size: 343
# [quoted lambdas] flat_size: 356
# [quoted lambdas] serialized term size: 1147
# [quoted lambdas] gzipped serialized term size: 382
# ASTcombined: 651
# [combined quoted lambdas] size: 530
# [combined quoted lambdas] flat_size: 586
# [combined quoted lambdas] serialized term size: 1955
# [combined quoted lambdas] gzipped serialized term size: 513
