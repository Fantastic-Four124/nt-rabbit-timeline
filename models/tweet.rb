class Tweet
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::Search

  field :contents, type: String
  field :date_posted, type: DateTime
  field :user, type: Hash
  field :date_posted, type: DateTime
  field :mentions, type: Array

  attr_readonly :user, :contents
  validates :user, presence: true
  search_in :contents, :hashtags
  store_in collection: 'nt-tweets'
end
