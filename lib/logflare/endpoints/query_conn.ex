defmodule Logflare.Endpoints.QueryConn do
  @moduledoc """
  The EndpointQuery connection. To be piped through functions like a Plug.Conn.
  """
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.User
  use TypedStruct

  typedstruct enforce: true do
    @typedoc "An EndpointQuery connection"
    field :endpoint_query, EndpointQuery.t()
    field :user, User.t(), default: nil
    # SQL string
    field :query_input, String.t()
    # a string map of parameters e.g. %{"value" => 123}
    field :input_params, map(), default: %{}
    # the list of declared parameters, e.g. @value would be ["value"]
    field :declared_params, [String.t()], default: []
    # the query response rows
    field :rows, [map()] | nil
  end
end
