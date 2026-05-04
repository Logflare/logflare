defmodule Logflare.EnumDeepUpdateTest do
  use ExUnit.Case
  import Logflare.EnumDeepUpdate

  describe "EnumDeepUpdate" do
    test "update_all_values_deep/2" do
      data = %{
        f1: [
          %{
            f2: [
              [%{f31: 1}, %{f31: 2}]
            ],
            f21: 1
          }
        ],
        f11: %{
          f22: %{
            f32: [1, 1, 1, 1, 2, 2, 2]
          },
          f23: 1
        }
      }

      fun = fn
        1 -> 0
        x -> x
      end

      assert %{
               f1: [%{f2: [[%{f31: 0}, %{f31: 2}]], f21: 0}],
               f11: %{f22: %{f32: [0, 0, 0, 0, 2, 2, 2]}, f23: 0}
             } == update_all_values_deep(data, fun)
    end
  end
end
