defmodule Logflare.Utils.ListTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @subject Logflare.Utils.List

  doctest @subject

  describe "exactly?/2" do
    test "empty list has length 0" do
      assert @subject.exactly?([], 0)
    end

    property "returns true if 2nd argument is equal to length" do
      check all(lst <- list_of(term())) do
        assert @subject.exactly?(lst, length(lst))
      end
    end

    property "return false if 2nd argument is greater than length" do
      check all(lst <- list_of(term()), delta <- positive_integer()) do
        refute @subject.exactly?(lst, length(lst) + delta)
      end
    end

    property "return false if 2nd argument is less than length" do
      check all(
              lst <- list_of(term(), min_length: 2),
              len = length(lst),
              delta <- integer(1..(len - 1))
            ) do
        refute @subject.exactly?(lst, len - delta)
      end
    end
  end

  describe "at_least?/2" do
    property "any list has at most 0 elements" do
      check all(lst <- list_of(term())) do
        assert @subject.at_least?(lst, 0)
      end
    end

    property "list has at most `length(list)` elements" do
      check all(lst <- list_of(term())) do
        assert @subject.at_least?(lst, length(lst))
      end
    end

    property "return false if 2nd argument is greater than length" do
      check all(lst <- list_of(term()), delta <- positive_integer()) do
        refute @subject.at_least?(lst, length(lst) + delta)
      end
    end

    property "list has at most `length(list) - delta` elements" do
      check all(
              lst <- list_of(term(), min_length: 2),
              len = length(lst),
              delta <- integer(1..(len - 1))
            ) do
        assert @subject.at_least?(lst, len - delta)
      end
    end
  end
end
