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

  def schema_factory(%{variant: :third_with_lists}) do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "datacenters",
              policyTags: nil,
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "ip_address",
              policyTags: nil,
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "request_method",
              policyTags: nil,
              type: "STRING"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: [
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: [
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      categories: nil,
                      description: nil,
                      fields: nil,
                      mode: "REPEATED",
                      name: "cities",
                      policyTags: nil,
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      categories: nil,
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "st",
                      policyTags: nil,
                      type: "STRING"
                    },
                    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                      categories: nil,
                      description: nil,
                      fields: nil,
                      mode: "NULLABLE",
                      name: "street",
                      policyTags: nil,
                      type: "STRING"
                    }
                  ],
                  mode: "REPEATED",
                  name: "address",
                  policyTags: nil,
                  type: "RECORD"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "browser",
                  policyTags: nil,
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "company",
                  policyTags: nil,
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "REPEATED",
                  name: "ids",
                  policyTags: nil,
                  type: "INTEGER"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "REPEATED",
                  name: "last_login_datetimes",
                  policyTags: nil,
                  type: "STRING"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "login_count",
                  policyTags: nil,
                  type: "INTEGER"
                },
                %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                  categories: nil,
                  description: nil,
                  fields: nil,
                  mode: "NULLABLE",
                  name: "vip",
                  policyTags: nil,
                  type: "BOOLEAN"
                }
              ],
              mode: "REPEATED",
              name: "user",
              policyTags: nil,
              type: "RECORD"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          policyTags: nil,
          type: "RECORD"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          policyTags: nil,
          type: "TIMESTAMP"
        }
      ]
    }
  end

  def metadata_factory(%{variant: :third}) do
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
  end

  def metadata_factory(%{variant: :third_with_lists}) do
    %{
      "ip_address" => "100.100.100.100",
      "datacenters" => "aws",
      "request_method" => "POST",
      "user" => %{
        "address" => %{
          "cities" => ["New York", "Denver", "Fargo"],
          "st" => "NY",
          "street" => "123 W Main St"
        },
        "browser" => "Firefox",
        "company" => "Apple",
        "ids" => [299, 38, 12, 55],
        "last_login_datetimes" => [
          "2020-01-01T00:00:01Z",
          "2020-01-01T00:01:00Z",
          "2020-01-01T01:00:00Z"
        ],
        "login_count" => 154,
        "vip" => true
      }
    }
  end
end
