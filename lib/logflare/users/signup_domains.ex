defmodule Logflare.Users.SignupDomains do
  @moduledoc """
  Restricts new accounts to a configured list of email domains.

  The list comes from the `LOGFLARE_SIGNUP_ALLOWED_DOMAINS` environment variable, for
  example `supabase.com,supabase.io`. An empty list allows every domain.

  Only a new user or a new team member is checked. An existing account can sign in from
  any domain. The match is exact and case-insensitive: `mail.supabase.com` does not
  match `supabase.com`.
  """

  @rejection_message "New accounts are restricted to approved email domains."
  @domain_pattern ~r/\A[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\z/
  @email_domain_pattern ~r/\A.+@([^@]+)\z/

  @doc """
  Parses a comma-separated list of domains.

  Raises `ArgumentError` for an entry that is not a domain name. A malformed entry
  matches no email address, so it would block every signup without a signal.

  ## Examples

      iex> Logflare.Users.SignupDomains.parse!("supabase.com, Supabase.IO ,@example.com,,")
      ["supabase.com", "supabase.io", "example.com"]

      iex> Logflare.Users.SignupDomains.parse!(nil)
      []

      iex> Logflare.Users.SignupDomains.parse!("supabase.com;supabase.io")
      ** (ArgumentError) invalid domain "supabase.com;supabase.io" in LOGFLARE_SIGNUP_ALLOWED_DOMAINS. Use a comma-separated list of domain names, for example "supabase.com,supabase.io"
  """
  @spec parse!(String.t() | nil) :: [String.t()]
  def parse!(nil), do: []

  def parse!(value) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&normalize_domain/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.map(&validate_domain!/1)
  end

  @doc """
  Message for a person whose signup is rejected.
  """
  @spec rejection_message() :: String.t()
  def rejection_message, do: @rejection_message

  @doc """
  Domains that can create a new account. An empty list means no restriction.
  """
  @spec allowed_domains() :: [String.t()]
  def allowed_domains, do: Application.get_env(:logflare, :signup_allowed_domains, [])

  @doc """
  Returns true if an account with this email address can be created.
  """
  @spec allowed?(String.t() | nil) :: boolean()
  def allowed?(email), do: allowed?(email, __MODULE__.allowed_domains())

  @spec allowed?(String.t() | nil, [String.t()]) :: boolean()
  defp allowed?(_email, []), do: true

  defp allowed?(email, domains) when is_binary(email) do
    normalized = email |> String.trim() |> String.downcase()

    case Regex.run(@email_domain_pattern, normalized) do
      [_email, domain] -> domain in domains
      nil -> false
    end
  end

  defp allowed?(_email, _domains), do: false

  @spec validate_domain!(String.t()) :: String.t()
  defp validate_domain!(domain) do
    if Regex.match?(@domain_pattern, domain) do
      domain
    else
      raise ArgumentError,
            "invalid domain #{inspect(domain)} in LOGFLARE_SIGNUP_ALLOWED_DOMAINS. " <>
              "Use a comma-separated list of domain names, for example \"supabase.com,supabase.io\""
    end
  end

  @spec normalize_domain(String.t()) :: String.t()
  defp normalize_domain(domain) do
    domain
    |> String.trim()
    |> String.downcase()
    |> String.trim_leading("@")
  end
end
