defmodule Logflare.Logs.IngestTransformerTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.IngestTransformers

  describe "BQ spec transformations" do
    test "transformations order is correct" do
      log_params = %{"metadata" => %{"level1" => %{"level2" => %{"status-code" => 200}}}}

      assert transform(log_params, :to_bigquery_column_spec) == %{
               "metadata" => %{"level1" => %{"level2" => %{"status_code" => 200}}}
             }
    end

    @batch [
      %{
        "metadata" => %{
          "level-1-dashed-key" => "value",
          "level1" => %{
            "level2" => %{
              "dashed-key" => "value"
            }
          }
        }
      },
      %{
        "metadata" => %{
          "level1" => [
            %{
              "level2" => %{
                "dashed-key-more-dashes" => "value"
              }
            }
          ]
        }
      },
      %{
        "metadata" => %{
          "level1" => [
            %{
              "level2" => %{
                "dashed-key-more-dashes" => "value"
              }
            }
          ]
        }
      }
    ]
    test "dashes to underscores" do
      assert Enum.map(@batch, &transform(&1, [:dashes_to_underscores])) == [
               %{
                 "metadata" => %{
                   "level_1_dashed_key" => "value",
                   "level1" => %{"level2" => %{"dashed_key" => "value"}}
                 }
               },
               %{
                 "metadata" => %{
                   "level1" => [%{"level2" => %{"dashed_key_more_dashes" => "value"}}]
                 }
               },
               %{
                 "metadata" => %{
                   "level1" => [%{"level2" => %{"dashed_key_more_dashes" => "value"}}]
                 }
               }
             ]
    end

    @batch [
      %{
        "metadata" => %{
          "1level_key" => "value",
          "1level_key" => %{
            "2level_key" => %{
              "3level_key" => "value"
            }
          }
        }
      },
      %{
        "metadata" => %{
          "1level_key" => [
            %{
              "2level_key" => %{
                "3level_key" => "value"
              }
            }
          ]
        }
      },
      %{
        "metadata" => %{
          "1level" => [
            %{
              "2level" => %{
                "311level_key" => "value",
                3 => "value"
              }
            }
          ]
        }
      }
    ]
    test "alter leading numbers" do
      assert Enum.map(@batch, &transform(&1, [:alter_leading_numbers])) == [
               %{
                 "metadata" => %{
                   "onelevel_key" => %{
                     "twolevel_key" => %{"threelevel_key" => "value"}
                   }
                 }
               },
               %{
                 "metadata" => %{
                   "onelevel_key" => [
                     %{
                       "twolevel_key" => %{
                         "threelevel_key" => "value"
                       }
                     }
                   ]
                 }
               },
               %{
                 "metadata" => %{
                   "onelevel" => [
                     %{
                       "twolevel" => %{
                         3 => "value",
                         "three11level_key" => "value"
                       }
                     }
                   ]
                 }
               }
             ]
    end

    @batch [
      %{
        "metadata" => %{
          "level_1_key_!@#$%%^&*(" => %{
            "level_2_key+{}:\"<>?\"" => %{"threelevel_key" => "value"}
          }
        }
      },
      %{
        "metadata" => %{
          "1level_key" => [
            %{
              "2level_key" => %{
                "3level_key ,.~" => "value"
              }
            }
          ]
        }
      },
      %{
        "metadata" => %{
          "1level" => [
            %{
              "2level" => %{
                "3!!level_key" => "value"
              }
            }
          ]
        }
      }
    ]
    test "alphanumeric only" do
      assert Enum.map(@batch, &transform(&1, [:alphanumeric_only])) == [
               %{
                 "metadata" => %{
                   "level_1_key_" => %{
                     "level_2_key" => %{"threelevel_key" => "value"}
                   }
                 }
               },
               %{
                 "metadata" => %{
                   "1level_key" => [
                     %{"2level_key" => %{"3level_key" => "value"}}
                   ]
                 }
               },
               %{
                 "metadata" => %{
                   "1level" => [
                     %{"2level" => %{"3level_key" => "value"}}
                   ]
                 }
               }
             ]
    end

    @batch [
      %{
        "metadata" => %{
          "level_1_key_" => %{
            "_FILE_level_2_key_FILE_" => %{"threelevel_key" => "value"}
          }
        }
      },
      %{
        "metadata" => %{
          "_PARTITION_1level_key" => [
            %{"_FILE_2level_key_PARTITION_" => %{"3level_key" => "value"}}
          ]
        }
      },
      %{
        "metadata" => %{
          "_TABLE_1level" => [
            %{"2level" => %{"3level_key" => "value"}}
          ]
        }
      }
    ]
    test "strip bq prefixes" do
      assert Enum.map(@batch, &transform(&1, [:strip_bq_prefixes])) == [
               %{
                 "metadata" => %{
                   "level_1_key_" => %{
                     "level_2_key_FILE_" => %{"threelevel_key" => "value"}
                   }
                 }
               },
               %{
                 "metadata" => %{
                   "1level_key" => [%{"2level_key_PARTITION_" => %{"3level_key" => "value"}}]
                 }
               },
               %{
                 "metadata" => %{
                   "1level" => [%{"2level" => %{"3level_key" => "value"}}]
                 }
               }
             ]
    end

    @batch [
      %{
        "metadata" => %{
          "123456789" => %{
            "12345678901234" => %{"12345" => "value"}
          }
        }
      },
      %{
        "metadata" => %{
          "123456789" => %{
            "12345678901234" => %{"12345" => "value"}
          }
        }
      },
      %{
        "metadata" => %{
          "123456789" => [
            %{
              "12345678901234" => %{"12345" => "value"}
            }
          ]
        }
      }
    ]

    test "max length" do
      assert Enum.map(@batch, &transform(&1, [{:field_length, max: 5}])) == [
               %{
                 "metadata" => %{"12345" => %{"12345" => %{"12345" => "value"}}}
               },
               %{
                 "metadata" => %{"12345" => %{"12345" => %{"12345" => "value"}}}
               },
               %{
                 "metadata" => %{"12345" => [%{"12345" => %{"12345" => "value"}}]}
               }
             ]
    end
  end
end
