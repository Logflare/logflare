defmodule Logflare.Generators do
  @moduledoc false
  @predicates File.read!("priv/generators/predicates.txt")
  @objects File.read!("priv/generators/objects.txt")
  @teams File.read!("priv/generators/teams.txt")

  def team_name do
    opts = [capitalize: true]

    predicate(opts) <> " " <> team(opts)
  end

  def predicate(capitalize: true) do
    predicate()
    |> String.capitalize()
  end

  def predicate do
    @predicates
    |> String.split("\n", trim: true)
    |> Enum.random()
  end

  def object(capitalize: true) do
    object()
    |> String.capitalize()
  end

  def object do
    @objects
    |> String.split("\n", trim: true)
    |> Enum.random()
  end

  def team(capitalize: true) do
    team()
    |> String.capitalize()
  end

  def team do
    @teams
    |> String.split("\n", trim: true)
    |> Enum.random()
  end
end
