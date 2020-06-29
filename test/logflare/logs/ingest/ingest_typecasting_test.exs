defmodule Logflare.Logs.IngestTypecastingTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.IngestTypecasting

  describe "maybe cast batch" do
    test "batch 1" do
      batch = [
        %{
          "message" => "message 1",
          "metadata" => %{
            "key1" => %{
              "key2" => [
                %{"key3" => "3.1415"},
                %{"key3" => "3.1415"},
                %{"key3.1" => "just a string"}
              ]
            }
          }
        },
        %{
          "message" => "message 2",
          "metadata" => %{
            "key1.1" => %{
              "key2.1" => [
                %{"key3" => "3.1415"},
                %{"key3" => "3.1415"},
                %{"key3.1" => "just a string"}
              ]
            }
          }
        },
        %{
          "message" => "message 3",
          "metadata" => %{
            "key1.1" => [
              %{
                "key2.1" => [
                  %{"key3" => "3.1415"},
                  %{"key3" => "3.1415"},
                  %{"key3.1" => "just a string"}
                ]
              }
            ]
          }
        }
      ]

      typecasts = [
        %{
          from: "string",
          to: "float",
          path: ["metadata", "key1", "key2", "key3"]
        },
        %{
          from: "string",
          to: "float",
          path: ["metadata", "key1.1", "key2.1", "key3"]
        }
      ]

      result = maybe_cast_batch(batch, %{"schemaTypecasts" => typecasts})

      assert result == [
               %{
                 "message" => "message 1",
                 "metadata" => %{
                   "key1" => %{
                     "key2" => [
                       %{"key3" => 3.1415},
                       %{"key3" => 3.1415},
                       %{"key3.1" => "just a string"}
                     ]
                   }
                 }
               },
               %{
                 "message" => "message 2",
                 "metadata" => %{
                   "key1.1" => %{
                     "key2.1" => [
                       %{"key3" => 3.1415},
                       %{"key3" => 3.1415},
                       %{"key3.1" => "just a string"}
                     ]
                   }
                 }
               },
               %{
                 "message" => "message 3",
                 "metadata" => %{
                   "key1.1" => [
                     %{
                       "key2.1" => [
                         %{"key3" => 3.1415},
                         %{"key3" => 3.1415},
                         %{"key3.1" => "just a string"}
                       ]
                     }
                   ]
                 }
               }
             ]
    end
  end

  describe "typecasting strings to numbers" do
    test "strings to numbers" do
      casted =
        cast(
          %{
            "metadata" => %{
              "key1" => "10000.00001"
            }
          },
          [
            %{
              from: "string",
              to: "float",
              path: ["metadata", "key1"]
            }
          ]
        )

      assert casted == %{"metadata" => %{"key1" => 10000.00001}}
    end

    test "nested strings to numbers" do
      casted =
        cast(
          %{
            "metadata" => %{
              "key1" => "10000.00001",
              "key2" => %{
                "key3.1" => "3.1415",
                "key3" => [
                  %{
                    "key4" => "10000.000001"
                  },
                  %{
                    "key4.1" => "10000.0001"
                  }
                ]
              },
              "key3" => %{
                "key4" => "900.009"
              }
            }
          },
          [
            %{
              from: "string",
              to: "float",
              path: ["metadata", "key2", "key3.1"]
            },
            %{
              from: "string",
              to: "float",
              path: ["metadata", "key2", "key3", "key4"]
            },
            %{
              from: "string",
              to: "float",
              path: ["metadata", "key1"]
            }
          ]
        )

      assert casted == %{
               "metadata" => %{
                 "key1" => 10000.00001,
                 "key2" => %{
                   "key3" => [
                     %{"key4" => 10000.000001},
                     %{"key4.1" => "10000.0001"}
                   ],
                   "key3.1" => 3.1415
                 },
                 "key3" => %{"key4" => "900.009"}
               }
             }
    end
  end
end
