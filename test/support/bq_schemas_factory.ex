defmodule Logflare.Google.BigQuery.SchemaFactory do
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  use ExMachina

  def schema_factory(%{variant: :third}) do
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
                  type: "BOOLEAN"
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

  def metadata_factory(%{variant: :third}) do
    %{
      "event_message" => "This is an example.",
      "metadata" => [
        %{
          "ip_address" => "100.100.100.100",
          "datacenter" => "aws",
          "request_method" => "POST",
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
end
