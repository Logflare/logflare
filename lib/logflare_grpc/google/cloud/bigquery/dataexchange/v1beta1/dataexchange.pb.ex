defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.State do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STATE_UNSPECIFIED, 0
  field :ACTIVE, 1
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.Category do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :CATEGORY_UNSPECIFIED, 0
  field :CATEGORY_OTHERS, 1
  field :CATEGORY_ADVERTISING_AND_MARKETING, 2
  field :CATEGORY_COMMERCE, 3
  field :CATEGORY_CLIMATE_AND_ENVIRONMENT, 4
  field :CATEGORY_DEMOGRAPHICS, 5
  field :CATEGORY_ECONOMICS, 6
  field :CATEGORY_EDUCATION, 7
  field :CATEGORY_ENERGY, 8
  field :CATEGORY_FINANCIAL, 9
  field :CATEGORY_GAMING, 10
  field :CATEGORY_GEOSPATIAL, 11
  field :CATEGORY_HEALTHCARE_AND_LIFE_SCIENCE, 12
  field :CATEGORY_MEDIA, 13
  field :CATEGORY_PUBLIC_SECTOR, 14
  field :CATEGORY_RETAIL, 15
  field :CATEGORY_SPORTS, 16
  field :CATEGORY_SCIENCE_AND_RESEARCH, 17
  field :CATEGORY_TRANSPORTATION_AND_LOGISTICS, 18
  field :CATEGORY_TRAVEL_AND_TOURISM, 19
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :display_name, 2, type: :string, json_name: "displayName", deprecated: false
  field :description, 3, type: :string, deprecated: false
  field :primary_contact, 4, type: :string, json_name: "primaryContact", deprecated: false
  field :documentation, 5, type: :string, deprecated: false
  field :listing_count, 6, type: :int32, json_name: "listingCount", deprecated: false
  field :icon, 7, type: :bytes, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DataProvider do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :primary_contact, 2, type: :string, json_name: "primaryContact", deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.Publisher do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :primary_contact, 2, type: :string, json_name: "primaryContact", deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDatasetReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset_id, 1, type: :string, json_name: "datasetId", deprecated: false
  field :project_id, 2, type: :string, json_name: "projectId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDataset.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDataset do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset_reference, 1,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDatasetReference,
    json_name: "datasetReference",
    deprecated: false

  field :friendly_name, 2,
    type: Google.Protobuf.StringValue,
    json_name: "friendlyName",
    deprecated: false

  field :description, 3, type: Google.Protobuf.StringValue, deprecated: false

  field :labels, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDataset.LabelsEntry,
    map: true,
    deprecated: false

  field :location, 5, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.BigQueryDatasetSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:source, 0)

  field :bigquery_dataset, 6,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.BigQueryDatasetSource,
    json_name: "bigqueryDataset",
    oneof: 0,
    deprecated: false

  field :name, 1, type: :string, deprecated: false
  field :display_name, 2, type: :string, json_name: "displayName", deprecated: false
  field :description, 3, type: :string, deprecated: false
  field :primary_contact, 4, type: :string, json_name: "primaryContact", deprecated: false
  field :documentation, 5, type: :string, deprecated: false

  field :state, 7,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.State,
    enum: true,
    deprecated: false

  field :icon, 8, type: :bytes, deprecated: false

  field :data_provider, 9,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DataProvider,
    json_name: "dataProvider",
    deprecated: false

  field :categories, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing.Category,
    enum: true,
    deprecated: false

  field :publisher, 11,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Publisher,
    deprecated: false

  field :request_access, 12, type: :string, json_name: "requestAccess", deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListDataExchangesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListDataExchangesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_exchanges, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange,
    json_name: "dataExchanges"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListOrgDataExchangesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :organization, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListOrgDataExchangesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_exchanges, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange,
    json_name: "dataExchanges"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.GetDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.CreateDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :data_exchange_id, 2, type: :string, json_name: "dataExchangeId", deprecated: false

  field :data_exchange, 3,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange,
    json_name: "dataExchange",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.UpdateDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :update_mask, 1,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false

  field :data_exchange, 2,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange,
    json_name: "dataExchange",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DeleteDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListListingsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.ListListingsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :listings, 1, repeated: true, type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.GetListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.CreateListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :listing_id, 2, type: :string, json_name: "listingId", deprecated: false
  field :listing, 3, type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.UpdateListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :update_mask, 1,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false

  field :listing, 2, type: Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.DeleteListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.SubscribeListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:destination, 0)

  field :destination_dataset, 3,
    type: Google.Cloud.Bigquery.Dataexchange.V1beta1.DestinationDataset,
    json_name: "destinationDataset",
    oneof: 0

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.SubscribeListingResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.AnalyticsHubService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.dataexchange.v1beta1.AnalyticsHubService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :ListDataExchanges,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListDataExchangesRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListDataExchangesResponse
  )

  rpc(
    :ListOrgDataExchanges,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListOrgDataExchangesRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListOrgDataExchangesResponse
  )

  rpc(
    :GetDataExchange,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.GetDataExchangeRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange
  )

  rpc(
    :CreateDataExchange,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.CreateDataExchangeRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange
  )

  rpc(
    :UpdateDataExchange,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.UpdateDataExchangeRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.DataExchange
  )

  rpc(
    :DeleteDataExchange,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.DeleteDataExchangeRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :ListListings,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListListingsRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.ListListingsResponse
  )

  rpc(
    :GetListing,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.GetListingRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing
  )

  rpc(
    :CreateListing,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.CreateListingRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing
  )

  rpc(
    :UpdateListing,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.UpdateListingRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.Listing
  )

  rpc(
    :DeleteListing,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.DeleteListingRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :SubscribeListing,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.SubscribeListingRequest,
    Google.Cloud.Bigquery.Dataexchange.V1beta1.SubscribeListingResponse
  )

  rpc(:GetIamPolicy, Google.Iam.V1.GetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(:SetIamPolicy, Google.Iam.V1.SetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(
    :TestIamPermissions,
    Google.Iam.V1.TestIamPermissionsRequest,
    Google.Iam.V1.TestIamPermissionsResponse
  )
end

defmodule Google.Cloud.Bigquery.Dataexchange.V1beta1.AnalyticsHubService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Dataexchange.V1beta1.AnalyticsHubService.Service
end
