defmodule Logflare.TableBigQuerySchemaBuilderTest do
  alias Logflare.BigQuery.TableSchemaBuilder, as: SchemaBuilder
  use ExUnit.Case

  describe "schema builder" do
    test "correctly builds schema from first params metadata" do
      new =
        metadatas().first
        |> SchemaBuilder.build_table_schema(schemas().initial)

      expected = schemas().first
      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema from second params metadata" do
      new =
        metadatas().second
        |> SchemaBuilder.build_table_schema(schemas().first)

      expected = schemas().second
      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema from third params metadata" do
      new =
        metadatas().third
        |> SchemaBuilder.build_table_schema(schemas().second)

      expected = schemas().third

      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end
  end

  @doc """
  Utility function for removing everything except schemas names from TableFieldSchema structs
  for easier debugging of errors when not all fields schemas are present in the result
  """
  def deep_schema_to_field_names(%{fields: fields} = schema) when is_list(fields) do
    %{
      fields: Enum.map(fields, &deep_schema_to_field_names/1),
      name: Map.get(schema, :name, :top_level_schema)
    }
  end

  def deep_schema_to_field_names(%{name: name}) do
    %{name: name}
  end

  @doc """
  Utility function for a cleaner code
  """
  def schemas() do
    for id <- ~w(initial first second third)a, into: Map.new() do
      sorted_schema = id |> get_schema() |> SchemaBuilder.deep_sort_by_fields_name()
      {id, sorted_schema}
    end
  end

  @doc """
  Utility function for a cleaner code
  """
  def metadatas() do
    for id <- ~w(first second third)a, into: Map.new() do
      {id, get_params(id)["metadata"]}
    end
  end

  def get_params(:first) do
    %{
      "event_message" => "This is an example.",
      "metadata" => [
        %{
          "datacenter" => "aws",
          "ip_address" => "100.100.100.100",
          "request_method" => "POST"
        }
      ],
      "timestamp" => ~N[2019-04-12 16:38:37]
    }
  end

  def get_params(:second) do
    %{
      "event_message" => "This is an example.",
      "metadata" => [
        %{
          "datacenter" => "aws",
          "ip_address" => "100.100.100.100",
          "request_method" => "POST",
          "user" => %{
            "address" => %{
              "city" => "New York",
              "st" => "NY",
              "street" => "123 W Main St"
            },
            "browser" => "Firefox",
            "id" => 38,
            "vip" => true
          }
        }
      ],
      "timestamp" => ~N[2019-04-12 16:41:56]
    }
  end

  def get_params(:third) do
    %{
      "event_message" => "This is an example.",
      "metadata" => [
        %{
          "ip_address" => "100.100.100.100",
          "user" => %{
            "address" => %{
              "city" => "New York",
              "st" => "NY",
              "street" => "123 W Main St"
            },
            "browser" => "Firefox",
            "company" => "Apple",
            "id" => 38,
            "login_count" => 154,
            "vip" => true
          }
        }
      ],
      "timestamp" => ~N[2019-04-12 16:44:38]
    }
  end

  def get_schema(:initial) do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        }
      ]
    }
  end

  def get_schema(:first) do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          type: "RECORD"
        }
      ]
    }
  end

  def get_schema(:second) do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: [
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "browser",
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "id",
                  type: "INTEGER"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "vip",
                  type: "BOOLEAN"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: [
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "street",
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "city",
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "st",
                      type: "STRING"
                    }
                  ],
                  mode: "REPEATED",
                  name: "address",
                  type: "RECORD"
                }
              ],
              mode: "REPEATED",
              name: "user",
              type: "RECORD"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          type: "RECORD"
        }
      ]
    }
  end

  def get_schema(:third) do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              description: nil,
              fields: [
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "browser",
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "id",
                  type: "INTEGER"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "vip",
                  type: "BOOLEAN"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "company",
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "login_count",
                  type: "INTEGER"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  description: nil,
                  fields: [
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "street",
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "city",
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "st",
                      type: "STRING"
                    }
                  ],
                  mode: "REPEATED",
                  name: "address",
                  type: "RECORD"
                }
              ],
              mode: "REPEATED",
              name: "user",
              type: "RECORD"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          type: "RECORD"
        }
      ]
    }
  end
end
