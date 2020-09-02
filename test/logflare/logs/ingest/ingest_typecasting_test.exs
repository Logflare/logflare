defmodule Logflare.Logs.IngestTypecastingTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.IngestTypecasting

  describe "transform directives" do
    @metadata %{
      "field12" => [%{"field2" => [1, 1, 1.0, "1"]}],
      "field1" => %{
        "field2" => %{
          "number1" => 1.0,
          "number2" => 1
        }
      }
    }

    test "numbersToFloats" do
      log_params = %{
        "metadata" => @metadata,
        "message" => "test message",
        "timestamp" => 1_577_836_800_000,
        "@logflareTransformDirectives" => %{
          "numbersToFloats" => true
        }
      }

      assert maybe_apply_transform_directives(log_params) == %{
               "message" => "test message",
               "metadata" => %{
                 "field12" => [%{"field2" => [1.0, 1.0, 1.0, "1"]}],
                 "field1" => %{"field2" => %{"number1" => 1.0, "number2" => 1.0}}
               },
               "timestamp" => 1_577_836_800_000
             }
    end

    test "numbersToFloats with no directives" do
      log_params = %{
        "metadata" => @metadata,
        "message" => "test message",
        "timestamp" => 1_577_836_800_000
      }

      assert maybe_apply_transform_directives(log_params) == %{
               "metadata" => @metadata,
               "message" => "test message",
               "timestamp" => 1_577_836_800_000
             }
    end

    test "numbersToFloats with empty metadata" do
      log_params = %{
        "metadata" => %{},
        "message" => "test message",
        "timestamp" => 1_577_836_800_000,
        "@logflareTransformDirectives" => %{
          "numbersToFloats" => true
        }
      }

      assert maybe_apply_transform_directives(log_params) == %{
               "message" => "test message",
               "metadata" => %{},
               "timestamp" => 1_577_836_800_000
             }
    end
  end

  describe "maybe cast batch" do
    test "batch 1" do
      typecasts = [
        %{
          "from" => "string",
          "to" => "float",
          "path" => ["metadata", "key1", "key2", "key3"]
        },
        %{
          "from" => "string",
          "to" => "float",
          "path" => ["metadata", "key1.1", "key2.1", "key3"]
        }
      ]

      batch = [
        %{
          "body" => %{
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
          "typecasts" => typecasts
        },
        %{
          "body" => %{
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
          "typecasts" => typecasts
        },
        %{
          "body" => %{
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
          },
          "typecasts" => typecasts
        }
      ]

      result = maybe_cast_batch(batch)

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
        maybe_cast_log_params(%{
          "body" => %{
            "metadata" => %{
              "key1" => "10000.00001"
            }
          },
          "typecasts" => [
            %{
              "from" => "string",
              "to" => "float",
              "path" => ["metadata", "key1"]
            }
          ]
        })

      assert casted == %{"metadata" => %{"key1" => 10000.00001}}
    end

    test "nested strings to numbers" do
      casted =
        maybe_cast_log_params(%{
          "body" => %{
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
          "typecasts" => [
            %{
              "from" => "string",
              "to" => "float",
              "path" => ["metadata", "key2", "key3.1"]
            },
            %{
              "from" => "string",
              "to" => "float",
              "path" => ["metadata", "key2", "key3", "key4"]
            },
            %{
              "from" => "string",
              "to" => "float",
              "path" => ["metadata", "key1"]
            }
          ]
        })

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
