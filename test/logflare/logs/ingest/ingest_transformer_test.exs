defmodule Logflare.Logs.IngestTransformerTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.IngestTransformers

  describe "BQ spec transformations" do
    test "transformations order is correct" do
      log_params = %{"metadata" => %{"level1" => %{"level2" => %{"status-code" => 200}}}}

      assert transform(log_params, :to_bigquery_column_spec) == %{
               "metadata" => %{"level1" => %{"level2" => %{"_status_code" => 200}}}
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
                   "level1" => %{"level2" => %{"_dashed_key" => "value"}},
                   "_level_1_dashed_key" => "value"
                 }
               },
               %{
                 "metadata" => %{
                   "level1" => [%{"level2" => %{"_dashed_key_more_dashes" => "value"}}]
                 }
               },
               %{
                 "metadata" => %{
                   "level1" => [%{"level2" => %{"_dashed_key_more_dashes" => "value"}}]
                 }
               }
             ]
    end

    @batch [
      %{
        "metadata" => %{
          # "1level_key" => "value",
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
                "312level_key" => "value"
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
                   "_1level_key" => %{"_2level_key" => %{"_3level_key" => "value"}}
                 }
               },
               %{
                 "metadata" => %{
                   "_1level_key" => [%{"_2level_key" => %{"_3level_key" => "value"}}]
                 }
               },
               %{
                 "metadata" => %{
                   "_1level" => [
                     %{"_2level" => %{"_311level_key" => "value", "_312level_key" => "value"}}
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
                   "_level_1_key___________" => %{
                     "_level_2_key_________" => %{"threelevel_key" => "value"}
                   }
                 }
               },
               %{
                 "metadata" => %{
                   "1level_key" => [%{"2level_key" => %{"_3level_key____" => "value"}}]
                 }
               },
               %{"metadata" => %{"1level" => [%{"2level" => %{"_3__level_key" => "value"}}]}}
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
                     "__FILE_level_2_key_FILE_" => %{"threelevel_key" => "value"}
                   }
                 }
               },
               %{
                 "metadata" => %{
                   "__PARTITION_1level_key" => [
                     %{"__FILE_2level_key_PARTITION_" => %{"3level_key" => "value"}}
                   ]
                 }
               },
               %{"metadata" => %{"__TABLE_1level" => [%{"2level" => %{"3level_key" => "value"}}]}}
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
               %{"_metad" => %{"_12345" => %{"_12345" => %{"12345" => "value"}}}},
               %{"_metad" => %{"_12345" => %{"_12345" => %{"12345" => "value"}}}},
               %{"_metad" => %{"_12345" => [%{"_12345" => %{"12345" => "value"}}]}}
             ]
    end
  end

  describe ":to_bigquery_column_spec fused pipeline" do
    test "already-valid keys pass through unchanged" do
      log = %{"safe_key" => %{"already_underscored" => "v"}}
      assert transform(log, :to_bigquery_column_spec) == log
    end

    test "non-binary keys are returned as-is" do
      log = %{:atom_key => "v", 42 => "v"}
      assert transform(log, :to_bigquery_column_spec) == log
    end

    test "empty-string keys pass through" do
      assert transform(%{"" => "v"}, :to_bigquery_column_spec) == %{"" => "v"}
    end

    test "strips _TABLE_, _FILE_, _PARTITION_ prefixes by prepending one underscore" do
      log = %{"_TABLE_x" => 1, "_FILE_x" => 1, "_PARTITION_x" => 1}

      assert transform(log, :to_bigquery_column_spec) == %{
               "__TABLE_x" => 1,
               "__FILE_x" => 1,
               "__PARTITION_x" => 1
             }
    end

    test "only treats reserved prefix when it appears at the start of the key" do
      assert transform(%{"x_TABLE_y" => 1}, :to_bigquery_column_spec) == %{"x_TABLE_y" => 1}
    end

    test "reserved prefix with empty suffix still gets prepended" do
      assert transform(%{"_TABLE_" => 1}, :to_bigquery_column_spec) == %{"__TABLE_" => 1}
    end

    test "leading digit gets a prepended underscore" do
      log = %{"1foo" => 1, "9bar" => 1}
      assert transform(log, :to_bigquery_column_spec) == %{"_1foo" => 1, "_9bar" => 1}
    end

    test "dashes are replaced with underscores and a leading underscore is prepended" do
      log = %{"a-b" => 1, "a-b-c" => 1, "--" => 1}

      assert transform(log, :to_bigquery_column_spec) == %{
               "_a_b" => 1,
               "_a_b_c" => 1,
               "___" => 1
             }
    end

    test "non-alphanumeric bytes are replaced and a leading underscore is prepended" do
      log = %{"foo!" => 1, "a b" => 1, "x.y" => 1}
      assert transform(log, :to_bigquery_column_spec) == %{"_foo_" => 1, "_a_b" => 1, "_x_y" => 1}
    end

    test "multibyte UTF-8 follows PCRE's byte-mode Latin-1 word classification" do
      # "é" is <<0xC3, 0xA9>>. PCRE's byte-mode \w considers Latin-1 letter-range
      # bytes (incl. 0xC3) "word" but treats 0xA9 as non-word. Only 0xA9 is
      # replaced, and the rule prepends a single underscore for the match.
      assert transform(%{"é" => 1}, :to_bigquery_column_spec) ==
               %{<<?_, 0xC3, ?_>> => 1}
    end

    test "dash prepend suppresses the leading-digit prepend" do
      assert transform(%{"1-key" => 1}, :to_bigquery_column_spec) == %{"_1_key" => 1}
    end

    test "bq prefix prepend suppresses the leading-digit prepend" do
      assert transform(%{"_TABLE_1foo" => 1}, :to_bigquery_column_spec) == %{"__TABLE_1foo" => 1}
    end

    test "bq prefix + dash both prepend (two leading underscores)" do
      assert transform(%{"_TABLE_-foo" => 1}, :to_bigquery_column_spec) ==
               %{"___TABLE__foo" => 1}
    end

    test "leading digit + non-alnum both prepend (two leading underscores)" do
      assert transform(%{"1key!" => 1}, :to_bigquery_column_spec) == %{"__1key_" => 1}
    end

    test "dash + non-alnum both prepend (two leading underscores)" do
      assert transform(%{"a-b!" => 1}, :to_bigquery_column_spec) == %{"__a_b_" => 1}
    end

    test "dash + non-alnum + leading-digit-suppressed (still two leading underscores)" do
      assert transform(%{"1-key!" => 1}, :to_bigquery_column_spec) == %{"__1_key_" => 1}
    end

    test "leading digit followed by multibyte produces digit-prepend + selective per-byte replacement" do
      # Same Latin-1 rule as above: 0xC3 is kept, 0xA9 becomes "_". The "1"
      # also triggers leading_digit_prepend, so two leading underscores stack.
      assert transform(%{"1é" => 1}, :to_bigquery_column_spec) ==
               %{<<?_, ?_, ?1, 0xC3, ?_>> => 1}
    end

    test "keys at the 128-byte limit pass through untouched" do
      key = String.duplicate("a", 128)
      assert transform(%{key => 1}, :to_bigquery_column_spec) == %{key => 1}
    end

    test "keys over 128 bytes are truncated and prefixed with underscore" do
      key = String.duplicate("a", 129)
      expected = "_" <> String.duplicate("a", 128)
      assert transform(%{key => 1}, :to_bigquery_column_spec) == %{expected => 1}
    end

    test "prefix-induced length growth triggers truncation, dropping the last byte" do
      base = "_TABLE_" <> String.duplicate("a", 121)
      assert byte_size(base) == 128
      expected = "_" <> "__TABLE_" <> String.duplicate("a", 120)
      assert byte_size(expected) == 129
      assert transform(%{base => 1}, :to_bigquery_column_spec) == %{expected => 1}
    end
  end
end
