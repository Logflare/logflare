defmodule Google.Cloud.Bigquery.Analyticshub.V1.DiscoveryType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DISCOVERY_TYPE_UNSPECIFIED, 0
  field :DISCOVERY_TYPE_PRIVATE, 1
  field :DISCOVERY_TYPE_PUBLIC, 2
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SharedResourceType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :SHARED_RESOURCE_TYPE_UNSPECIFIED, 0
  field :BIGQUERY_DATASET, 1
  field :PUBSUB_TOPIC, 2
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.State do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STATE_UNSPECIFIED, 0
  field :ACTIVE, 1
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.Category do
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

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo.GoogleCloudMarketplaceInfo.CommercialState do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :COMMERCIAL_STATE_UNSPECIFIED, 0
  field :ONBOARDING, 1
  field :ACTIVE, 2
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription.State do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STATE_UNSPECIFIED, 0
  field :STATE_ACTIVE, 1
  field :STATE_STALE, 2
  field :STATE_INACTIVE, 3
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DataExchange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :display_name, 2, type: :string, json_name: "displayName", deprecated: false
  field :description, 3, type: :string, deprecated: false
  field :primary_contact, 4, type: :string, json_name: "primaryContact", deprecated: false
  field :documentation, 5, type: :string, deprecated: false
  field :listing_count, 6, type: :int32, json_name: "listingCount", deprecated: false
  field :icon, 7, type: :bytes, deprecated: false

  field :sharing_environment_config, 8,
    type: Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig,
    json_name: "sharingEnvironmentConfig",
    deprecated: false

  field :discovery_type, 9,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DiscoveryType,
    json_name: "discoveryType",
    enum: true,
    deprecated: false

  field :log_linked_dataset_query_user_email, 10,
    proto3_optional: true,
    type: :bool,
    json_name: "logLinkedDatasetQueryUserEmail",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig.DefaultExchangeConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig.DcrExchangeConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :single_selected_resource_sharing_restriction, 1,
    proto3_optional: true,
    type: :bool,
    json_name: "singleSelectedResourceSharingRestriction",
    deprecated: false

  field :single_linked_dataset_per_cleanroom, 2,
    proto3_optional: true,
    type: :bool,
    json_name: "singleLinkedDatasetPerCleanroom",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:environment, 0)

  field :default_exchange_config, 1,
    type: Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig.DefaultExchangeConfig,
    json_name: "defaultExchangeConfig",
    oneof: 0

  field :dcr_exchange_config, 2,
    type: Google.Cloud.Bigquery.Analyticshub.V1.SharingEnvironmentConfig.DcrExchangeConfig,
    json_name: "dcrExchangeConfig",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DataProvider do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :primary_contact, 2, type: :string, json_name: "primaryContact", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Publisher do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :primary_contact, 2, type: :string, json_name: "primaryContact", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DestinationDatasetReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset_id, 1, type: :string, json_name: "datasetId", deprecated: false
  field :project_id, 2, type: :string, json_name: "projectId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset_reference, 1,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationDatasetReference,
    json_name: "datasetReference",
    deprecated: false

  field :friendly_name, 2,
    type: Google.Protobuf.StringValue,
    json_name: "friendlyName",
    deprecated: false

  field :description, 3, type: Google.Protobuf.StringValue, deprecated: false

  field :labels, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset.LabelsEntry,
    map: true,
    deprecated: false

  field :location, 5, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DestinationPubSubSubscription do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :pubsub_subscription, 1,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PubSubSubscription,
    json_name: "pubsubSubscription",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource.SelectedResource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:resource, 0)

  field :table, 1, type: :string, oneof: 0, deprecated: false
  field :routine, 2, type: :string, oneof: 0, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource.RestrictedExportPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :enabled, 1, type: Google.Protobuf.BoolValue, deprecated: false

  field :restrict_direct_table_access, 2,
    type: Google.Protobuf.BoolValue,
    json_name: "restrictDirectTableAccess",
    deprecated: false

  field :restrict_query_result, 3,
    type: Google.Protobuf.BoolValue,
    json_name: "restrictQueryResult",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset, 1, type: :string, deprecated: false

  field :selected_resources, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource.SelectedResource,
    json_name: "selectedResources",
    deprecated: false

  field :restricted_export_policy, 3,
    type:
      Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource.RestrictedExportPolicy,
    json_name: "restrictedExportPolicy",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.PubSubTopicSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :topic, 1, type: :string, deprecated: false

  field :data_affinity_regions, 2,
    repeated: true,
    type: :string,
    json_name: "dataAffinityRegions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.RestrictedExportConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :enabled, 3, type: :bool, deprecated: false

  field :restrict_direct_table_access, 1,
    type: :bool,
    json_name: "restrictDirectTableAccess",
    deprecated: false

  field :restrict_query_result, 2,
    type: :bool,
    json_name: "restrictQueryResult",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo.GoogleCloudMarketplaceInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :service, 1, proto3_optional: true, type: :string, deprecated: false

  field :commercial_state, 3,
    proto3_optional: true,
    type:
      Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo.GoogleCloudMarketplaceInfo.CommercialState,
    json_name: "commercialState",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :cloud_marketplace, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo.GoogleCloudMarketplaceInfo,
    json_name: "cloudMarketplace",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Listing do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:source, 0)

  field :bigquery_dataset, 6,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.BigQueryDatasetSource,
    json_name: "bigqueryDataset",
    oneof: 0

  field :pubsub_topic, 16,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.PubSubTopicSource,
    json_name: "pubsubTopic",
    oneof: 0

  field :name, 1, type: :string, deprecated: false
  field :display_name, 2, type: :string, json_name: "displayName", deprecated: false
  field :description, 3, type: :string, deprecated: false
  field :primary_contact, 4, type: :string, json_name: "primaryContact", deprecated: false
  field :documentation, 5, type: :string, deprecated: false

  field :state, 7,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.State,
    enum: true,
    deprecated: false

  field :icon, 8, type: :bytes, deprecated: false

  field :data_provider, 9,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DataProvider,
    json_name: "dataProvider",
    deprecated: false

  field :categories, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.Category,
    enum: true,
    deprecated: false

  field :publisher, 11, type: Google.Cloud.Bigquery.Analyticshub.V1.Publisher, deprecated: false
  field :request_access, 12, type: :string, json_name: "requestAccess", deprecated: false

  field :restricted_export_config, 13,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.RestrictedExportConfig,
    json_name: "restrictedExportConfig",
    deprecated: false

  field :discovery_type, 14,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DiscoveryType,
    json_name: "discoveryType",
    enum: true,
    deprecated: false

  field :resource_type, 15,
    type: Google.Cloud.Bigquery.Analyticshub.V1.SharedResourceType,
    json_name: "resourceType",
    enum: true,
    deprecated: false

  field :commercial_info, 17,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Listing.CommercialInfo,
    json_name: "commercialInfo",
    deprecated: false

  field :log_linked_dataset_query_user_email, 18,
    proto3_optional: true,
    type: :bool,
    json_name: "logLinkedDatasetQueryUserEmail",
    deprecated: false

  field :allow_only_metadata_sharing, 19,
    proto3_optional: true,
    type: :bool,
    json_name: "allowOnlyMetadataSharing",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription.LinkedResource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:reference, 0)

  field :linked_dataset, 1, type: :string, json_name: "linkedDataset", oneof: 0, deprecated: false

  field :linked_pubsub_subscription, 3,
    type: :string,
    json_name: "linkedPubsubSubscription",
    oneof: 0,
    deprecated: false

  field :listing, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription.CommercialInfo.GoogleCloudMarketplaceInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :order, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription.CommercialInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :cloud_marketplace, 1,
    type:
      Google.Cloud.Bigquery.Analyticshub.V1.Subscription.CommercialInfo.GoogleCloudMarketplaceInfo,
    json_name: "cloudMarketplace",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription.LinkedDatasetMapEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription.LinkedResource
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.Subscription do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:resource_name, 0)

  field :listing, 5, type: :string, oneof: 0, deprecated: false
  field :data_exchange, 6, type: :string, json_name: "dataExchange", oneof: 0, deprecated: false
  field :name, 1, type: :string, deprecated: false

  field :creation_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "creationTime",
    deprecated: false

  field :last_modify_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "lastModifyTime",
    deprecated: false

  field :organization_id, 4, type: :string, json_name: "organizationId", deprecated: false

  field :organization_display_name, 10,
    type: :string,
    json_name: "organizationDisplayName",
    deprecated: false

  field :state, 7,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription.State,
    enum: true,
    deprecated: false

  field :linked_dataset_map, 8,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription.LinkedDatasetMapEntry,
    json_name: "linkedDatasetMap",
    map: true,
    deprecated: false

  field :subscriber_contact, 9, type: :string, json_name: "subscriberContact", deprecated: false

  field :linked_resources, 11,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription.LinkedResource,
    json_name: "linkedResources",
    deprecated: false

  field :resource_type, 12,
    type: Google.Cloud.Bigquery.Analyticshub.V1.SharedResourceType,
    json_name: "resourceType",
    enum: true,
    deprecated: false

  field :commercial_info, 13,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription.CommercialInfo,
    json_name: "commercialInfo",
    deprecated: false

  field :log_linked_dataset_query_user_email, 14,
    proto3_optional: true,
    type: :bool,
    json_name: "logLinkedDatasetQueryUserEmail",
    deprecated: false

  field :destination_dataset, 15,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset,
    json_name: "destinationDataset",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListDataExchangesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListDataExchangesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_exchanges, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DataExchange,
    json_name: "dataExchanges"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListOrgDataExchangesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :organization, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListOrgDataExchangesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_exchanges, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DataExchange,
    json_name: "dataExchanges"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.GetDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.CreateDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :data_exchange_id, 2, type: :string, json_name: "dataExchangeId", deprecated: false

  field :data_exchange, 3,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DataExchange,
    json_name: "dataExchange",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.UpdateDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :update_mask, 1,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false

  field :data_exchange, 2,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DataExchange,
    json_name: "dataExchange",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DeleteDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListListingsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListListingsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :listings, 1, repeated: true, type: Google.Cloud.Bigquery.Analyticshub.V1.Listing
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.GetListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.CreateListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :listing_id, 2, type: :string, json_name: "listingId", deprecated: false
  field :listing, 3, type: Google.Cloud.Bigquery.Analyticshub.V1.Listing, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.UpdateListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :update_mask, 1,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false

  field :listing, 2, type: Google.Cloud.Bigquery.Analyticshub.V1.Listing, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DeleteListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :delete_commercial, 2, type: :bool, json_name: "deleteCommercial", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SubscribeListingRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:destination, 0)

  field :destination_dataset, 3,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset,
    json_name: "destinationDataset",
    oneof: 0,
    deprecated: false

  field :destination_pubsub_subscription, 5,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationPubSubSubscription,
    json_name: "destinationPubsubSubscription",
    oneof: 0,
    deprecated: false

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SubscribeListingResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :subscription, 1, type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SubscribeDataExchangeRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :destination, 2, type: :string, deprecated: false

  field :destination_dataset, 5,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DestinationDataset,
    json_name: "destinationDataset",
    deprecated: false

  field :subscription, 4, type: :string, deprecated: false
  field :subscriber_contact, 3, type: :string, json_name: "subscriberContact"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.SubscribeDataExchangeResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :subscription, 1, type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.RefreshSubscriptionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.RefreshSubscriptionResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :subscription, 1, type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.GetSubscriptionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListSubscriptionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :filter, 2, type: :string
  field :page_size, 3, type: :int32, json_name: "pageSize"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListSubscriptionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :subscriptions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListSharedResourceSubscriptionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: :string, deprecated: false
  field :include_deleted_subscriptions, 2, type: :bool, json_name: "includeDeletedSubscriptions"
  field :page_size, 3, type: :int32, json_name: "pageSize"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ListSharedResourceSubscriptionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :shared_resource_subscriptions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.Subscription,
    json_name: "sharedResourceSubscriptions"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.RevokeSubscriptionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :revoke_commercial, 2, type: :bool, json_name: "revokeCommercial", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.RevokeSubscriptionResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DeleteSubscriptionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.OperationMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :create_time, 1,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :end_time, 2, type: Google.Protobuf.Timestamp, json_name: "endTime", deprecated: false
  field :target, 3, type: :string, deprecated: false
  field :verb, 4, type: :string, deprecated: false
  field :status_message, 5, type: :string, json_name: "statusMessage", deprecated: false

  field :requested_cancellation, 6,
    type: :bool,
    json_name: "requestedCancellation",
    deprecated: false

  field :api_version, 7, type: :string, json_name: "apiVersion", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.AnalyticsHubService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.analyticshub.v1.AnalyticsHubService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :ListDataExchanges,
    Google.Cloud.Bigquery.Analyticshub.V1.ListDataExchangesRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.ListDataExchangesResponse
  )

  rpc(
    :ListOrgDataExchanges,
    Google.Cloud.Bigquery.Analyticshub.V1.ListOrgDataExchangesRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.ListOrgDataExchangesResponse
  )

  rpc(
    :GetDataExchange,
    Google.Cloud.Bigquery.Analyticshub.V1.GetDataExchangeRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.DataExchange
  )

  rpc(
    :CreateDataExchange,
    Google.Cloud.Bigquery.Analyticshub.V1.CreateDataExchangeRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.DataExchange
  )

  rpc(
    :UpdateDataExchange,
    Google.Cloud.Bigquery.Analyticshub.V1.UpdateDataExchangeRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.DataExchange
  )

  rpc(
    :DeleteDataExchange,
    Google.Cloud.Bigquery.Analyticshub.V1.DeleteDataExchangeRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :ListListings,
    Google.Cloud.Bigquery.Analyticshub.V1.ListListingsRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.ListListingsResponse
  )

  rpc(
    :GetListing,
    Google.Cloud.Bigquery.Analyticshub.V1.GetListingRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.Listing
  )

  rpc(
    :CreateListing,
    Google.Cloud.Bigquery.Analyticshub.V1.CreateListingRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.Listing
  )

  rpc(
    :UpdateListing,
    Google.Cloud.Bigquery.Analyticshub.V1.UpdateListingRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.Listing
  )

  rpc(
    :DeleteListing,
    Google.Cloud.Bigquery.Analyticshub.V1.DeleteListingRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :SubscribeListing,
    Google.Cloud.Bigquery.Analyticshub.V1.SubscribeListingRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.SubscribeListingResponse
  )

  rpc(
    :SubscribeDataExchange,
    Google.Cloud.Bigquery.Analyticshub.V1.SubscribeDataExchangeRequest,
    Google.Longrunning.Operation
  )

  rpc(
    :RefreshSubscription,
    Google.Cloud.Bigquery.Analyticshub.V1.RefreshSubscriptionRequest,
    Google.Longrunning.Operation
  )

  rpc(
    :GetSubscription,
    Google.Cloud.Bigquery.Analyticshub.V1.GetSubscriptionRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.Subscription
  )

  rpc(
    :ListSubscriptions,
    Google.Cloud.Bigquery.Analyticshub.V1.ListSubscriptionsRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.ListSubscriptionsResponse
  )

  rpc(
    :ListSharedResourceSubscriptions,
    Google.Cloud.Bigquery.Analyticshub.V1.ListSharedResourceSubscriptionsRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.ListSharedResourceSubscriptionsResponse
  )

  rpc(
    :RevokeSubscription,
    Google.Cloud.Bigquery.Analyticshub.V1.RevokeSubscriptionRequest,
    Google.Cloud.Bigquery.Analyticshub.V1.RevokeSubscriptionResponse
  )

  rpc(
    :DeleteSubscription,
    Google.Cloud.Bigquery.Analyticshub.V1.DeleteSubscriptionRequest,
    Google.Longrunning.Operation
  )

  rpc(:GetIamPolicy, Google.Iam.V1.GetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(:SetIamPolicy, Google.Iam.V1.SetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(
    :TestIamPermissions,
    Google.Iam.V1.TestIamPermissionsRequest,
    Google.Iam.V1.TestIamPermissionsResponse
  )
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.AnalyticsHubService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Analyticshub.V1.AnalyticsHubService.Service
end
