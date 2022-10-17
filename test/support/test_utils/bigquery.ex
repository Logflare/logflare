defmodule Logflare.TestUtils.BigQuery do
  @moduledoc false
  import ExUnit.Assertions
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaUtils

  def get_field(%TS{} = schema, str_path) when is_binary(str_path) do
    str_path
    |> String.split(".")
    |> Enum.reduce(schema, fn
      _key, nil -> nil
      key, %_{} = schema -> get_nested_field(schema, key)
    end)
  end

  defp get_nested_field(%_{fields: fields}, name) do
    Enum.find(fields, fn field -> field.name == name end)
  end

  @doc """
  Utility function for removing everything except schemas names from TableFieldSchema structs
  for easier debugging of errors when not all fields schemas are present in the result
  """
  def deep_schema_to_field_names(fields) when is_list(fields) do
    Enum.map(fields, &deep_schema_to_field_names/1)
  end

  def deep_schema_to_field_names(%{fields: fields} = schema) when is_list(fields) do
    %{
      Map.get(schema, :name, :top_level_schema) => Enum.map(fields, &deep_schema_to_field_names/1)
    }
  end

  def deep_schema_to_field_names(%{name: name}) do
    name
  end

  def assert_equal_schemas(schema_left, schema_right) do
    assert deep_schema_to_field_names(schema_left) == deep_schema_to_field_names(schema_right)
    assert schema_left == schema_right
  end

  @doc """
  Utility function for a cleaner code
  """
  def schemas() do
    for id <- ~w(initial second third list_of_maps)a, into: Map.new() do
      sorted_schema = id |> get_schema() |> SchemaUtils.deep_sort_by_fields_name()
      {id, sorted_schema}
    end
  end

  @doc """
  Utility function for a cleaner code
  """
  def metadatas() do
    for id <- ~w(third third_deep_nested_removed)a, into: Map.new() do
      {id, get_params(id)["metadata"]}
    end
  end

  def get_params(:third_deep_nested_removed) do
    %{
      "event_message" => "This is an example.",
      "id" => "uuid",
      "metadata" => [
        %{
          "datacenter" => "aws",
          "ip_address" => "100.100.100.100",
          # "request_method" => "POST",
          "user" => %{
            "address" => %{
              # "city" => "New York",
              "st" => "NY",
              "street" => "123 W Main St"
            },
            # "browser" => "Firefox",
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
      "id" => "uuid",
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


  def schema_and_payload_metadata(:list_of_maps_of_varying_shapes) do
    metadata = [
      %{
        "datacenter" => "aws",
        "ip_address" => "100.100.100.100",
        "request_method" => "POST",
        "miscellaneous" => [
          %{
            "string1key" => "string1val",
            "string2key" => "string2val"
          },
          %{
            "int1" => 1,
            "int2" => 2
          },
          %{
            "map1" => %{
              "nested_map_lvl_1" => 1,
              "nested_map_lvl_2" => [
                %{
                  "nested_map_lvl_4" => %{"string40key" => "string"}
                },
                %{
                  "nested_map_lvl_5" => %{"string5key" => "string"},
                  "nested_map_lvl_4" => %{"string41key" => "string"}
                },
                %{
                  "nested_map_lvl_4" => %{"string42key" => "string"},
                  "nested_map_lvl_6" => %{"string6key" => "string"}
                }
              ]
            },
            "int3" => 3
          }
        ]
      }
    ]

    schema = %TS{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          type: "STRING"
        },
        %TFS{
          description: nil,
          mode: "REPEATED",
          name: "metadata",
          type: "RECORD",
          fields: [
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %TFS{
              description: nil,
              mode: "REPEATED",
              name: "miscellaneous",
              type: "RECORD",
              fields: [
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "int1",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "int2",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "int3",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  mode: "REPEATED",
                  name: "map1",
                  type: "RECORD",
                  fields: [
                    %TFS{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "nested_map_lvl_1",
                      type: "INTEGER"
                    },
                    %TFS{
                      description: nil,
                      mode: "REPEATED",
                      name: "nested_map_lvl_2",
                      type: "RECORD",
                      fields: [
                        %TFS{
                          description: nil,
                          mode: "REPEATED",
                          name: "nested_map_lvl_4",
                          type: "RECORD",
                          fields: [
                            %TFS{
                              description: nil,
                              fields: nil,
                              mode: "NULLABLE",
                              type: "STRING",
                              name: "string40key"
                            },
                            %TFS{
                              description: nil,
                              fields: nil,
                              mode: "NULLABLE",
                              name: "string41key",
                              type: "STRING"
                            },
                            %TFS{
                              description: nil,
                              fields: nil,
                              mode: "NULLABLE",
                              name: "string42key",
                              type: "STRING"
                            }
                          ]
                        },
                        %TFS{
                          description: nil,
                          fields: [
                            %TFS{
                              description: nil,
                              fields: nil,
                              mode: "NULLABLE",
                              name: "string5key",
                              type: "STRING"
                            }
                          ],
                          mode: "REPEATED",
                          name: "nested_map_lvl_5",
                          type: "RECORD"
                        },
                        %TFS{
                          description: nil,
                          fields: [
                            %TFS{
                              description: nil,
                              fields: nil,
                              mode: "NULLABLE",
                              name: "string6key",
                              type: "STRING"
                            }
                          ],
                          mode: "REPEATED",
                          name: "nested_map_lvl_6",
                          type: "RECORD"
                        }
                      ]
                    }
                  ]
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "string1key",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "string2key",
                  type: "STRING"
                }
              ]
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            }
          ]
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        }
      ]
    }

    %{metadata: metadata, schema: schema}
  end

  def get_schema(:initial) do
    SchemaBuilder.initial_table_schema()
  end

  def get_schema(:second) do
    %TS{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: [
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: [
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "browser",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "id",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "vip",
                  type: "BOOL"
                },
                %TFS{
                  description: nil,
                  fields: [
                    %TFS{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "street",
                      type: "STRING"
                    },
                    %TFS{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "city",
                      type: "STRING"
                    },
                    %TFS{
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
    %TS{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: [
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: [
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "browser",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "id",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "vip",
                  type: "BOOL"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "company",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "login_count",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: [
                    %TFS{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "street",
                      type: "STRING"
                    },
                    %TFS{
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "city",
                      type: "STRING"
                    },
                    %TFS{
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

  def get_schema(:list_of_maps) do
    %TS{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: [
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenter",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: [
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "arity_or_args",
                  type: "INTEGER"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "function",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "file",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "module",
                  type: "STRING"
                },
                %TFS{
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "line",
                  type: "INTEGER"
                }
              ],
              mode: "REPEATED",
              name: "stacktrace",
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
