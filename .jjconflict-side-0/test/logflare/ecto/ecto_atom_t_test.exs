defmodule Ecto.AtomTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  describe "type/0" do
    test "returns :string" do
      assert Ecto.Atom.type() == :string
    end
  end

  describe "cast/1" do
    property "casting an atom always succeeds" do
      check all atom <- atom(:alphanumeric) do
        assert Ecto.Atom.cast(atom) == {:ok, atom}
      end
    end

    property "casting non-atoms always fails" do
      check all value <- term(), not is_atom(value) do
        assert Ecto.Atom.cast(value) == :error
      end
    end
  end

  describe "load/1" do
    property "load roundtrip with dump preserves atoms" do
      check all atom <- atom(:alphanumeric) do
        assert {:ok, string} = Ecto.Atom.dump(atom)
        assert is_binary(string)
        assert {:ok, ^atom} = Ecto.Atom.load(string)
      end
    end

    test "returns error for invalid data" do
      assert {:error, %ArgumentError{}} = Ecto.Atom.load(nil)
      assert {:error, %ArgumentError{}} = Ecto.Atom.load(<<1, 2, 3>>)
    end

    test "returns error for non existing atom" do
      assert {:error, %ArgumentError{}} =
               Ecto.Atom.load("non_existing_atom_ever_#{System.unique_integer()}")
    end
  end

  describe "dump/1" do
    property "converts atoms to strings" do
      check all atom <- atom(:alphanumeric) do
        assert {:ok, string} = Ecto.Atom.dump(atom)
        assert is_binary(string)
        assert string == Atom.to_string(atom)
      end
    end

    test "returns :error for non-atoms" do
      assert Ecto.Atom.dump(123) == :error
      assert Ecto.Atom.dump("string") == :error
      assert Ecto.Atom.dump([:list]) == :error
      assert Ecto.Atom.dump(%{}) == :error
    end
  end

  describe "embed_as/1" do
    property "returns :self for any input" do
      check all value <- term() do
        assert Ecto.Atom.embed_as(value) == :self
      end
    end
  end

  describe "equal?/2" do
    property "is reflexive" do
      check all atom <- atom(:alphanumeric) do
        assert Ecto.Atom.equal?(atom, atom) == true
      end
    end

    property "is symmetric" do
      check all atom1 <- atom(:alphanumeric),
                atom2 <- atom(:alphanumeric) do
        assert Ecto.Atom.equal?(atom1, atom2) == Ecto.Atom.equal?(atom2, atom1)
      end
    end

    property "equal? returns false for different atoms" do
      check all atom1 <- atom(:alphanumeric), atom2 <- atom(:alphanumeric), atom1 != atom2 do
        assert Ecto.Atom.equal?(atom1, atom2) == false
      end
    end
  end

  property "cast -> dump -> load preserves data when initial input is an atom" do
    check all atom <- atom(:alphanumeric) do
      assert {:ok, ^atom} = Ecto.Atom.cast(atom)
      assert {:ok, string} = Ecto.Atom.dump(atom)
      assert {:ok, ^atom} = Ecto.Atom.load(string)
    end
  end
end
