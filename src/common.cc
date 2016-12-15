/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>
#include <vector>

#include "src/common.h"

namespace NodeKafka {

void Log(std::string str) {
  std::cerr << "% " << str.c_str() << std::endl;
}

template<typename T>
T GetParameter(v8::Local<v8::Object> object, std::string field_name, T def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    Nan::Maybe<T> maybeT = Nan::To<T>(Nan::Get(object, field).ToLocalChecked());
    if (maybeT.IsNothing()) {
      return def;
    } else {
      return maybeT.FromJust();
    }
  }
  return def;
}

template<>
int64_t GetParameter<int64_t>(v8::Local<v8::Object> object,
  std::string field_name, int64_t def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> v = Nan::Get(object, field).ToLocalChecked();

    if (!v->IsNumber()) {
      return def;
    }

    Nan::Maybe<int64_t> maybeInt = Nan::To<int64_t>(v);
    if (maybeInt.IsNothing()) {
      return def;
    } else {
      return maybeInt.FromJust();
    }
  }
  return def;
}

template<>
int GetParameter<int>(v8::Local<v8::Object> object,
  std::string field_name, int def) {
  return static_cast<int>(GetParameter<int64_t>(object, field_name, def));
}

template<>
std::string GetParameter<std::string>(v8::Local<v8::Object> object,
                                      std::string field_name,
                                      std::string def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();
  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> parameter =
      Nan::Get(object, field).ToLocalChecked();
      // Nan::To<v8::String>();

    if (!parameter->IsUndefined() && !parameter->IsNull()) {
      v8::Local<v8::String> val = parameter->ToString();

      if (!val->IsUndefined() && !val->IsNull()) {
        Nan::Utf8String parameterValue(val);
        std::string parameterString(*parameterValue);

        return parameterString;
      }
    }
  }
  return def;
}

template<>
std::vector<std::string> GetParameter<std::vector<std::string> >(
  v8::Local<v8::Object> object, std::string field_name,
  std::vector<std::string> def) {
  v8::Local<v8::String> field = Nan::New(field_name.c_str()).ToLocalChecked();

  if (Nan::Has(object, field).FromMaybe(false)) {
    v8::Local<v8::Value> maybeArray = Nan::Get(object, field).ToLocalChecked();
    if (maybeArray->IsArray()) {
      v8::Local<v8::Array> parameter = maybeArray.As<v8::Array>();
      return v8ArrayToStringVector(parameter);
    }
  }
  return def;
}

std::vector<std::string> v8ArrayToStringVector(v8::Local<v8::Array> parameter) {
  std::vector<std::string> newItem;

  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      Nan::MaybeLocal<v8::String> p = Nan::To<v8::String>(parameter->Get(i));
      if (p.IsEmpty()) {
        continue;
      }
      Nan::Utf8String pVal(p.ToLocalChecked());
      std::string pString(*pVal);
      newItem.push_back(pString);
    }
  }
  return newItem;
}

namespace Conversion {
namespace Topic {

std::vector<std::string> ToStringVector(v8::Local<v8::Array> parameter) {
  std::vector<std::string> newItem;

  if (parameter->Length() >= 1) {
    for (unsigned int i = 0; i < parameter->Length(); i++) {
      v8::Local<v8::Value> element = parameter->Get(i);

      if (!element->IsRegExp()) {
        Nan::MaybeLocal<v8::String> p = Nan::To<v8::String>(element);

        if (p.IsEmpty()) {
          continue;
        }

        Nan::Utf8String pVal(p.ToLocalChecked());
        std::string pString(*pVal);

        newItem.push_back(pString);
      } else {
        Nan::Utf8String pVal(element.As<v8::RegExp>()->GetSource());
        std::string pString(*pVal);

        // If it is a regular expression wrap it in that kafka decoration
        std::string regexString = "\"" + pString + "\"";

        newItem.push_back(pString);
      }
    }
  }

  return newItem;
}

v8::Local<v8::Array> ToV8Array(std::vector<std::string> parameter) {
  v8::Local<v8::Array> newItem = Nan::New<v8::Array>();

  for (size_t i = 0; i < parameter.size(); i++) {
    std::string topic = parameter[i];

    newItem->Set(i, Nan::New<v8::String>(topic).ToLocalChecked());
  }

  return newItem;
}

}  // namespace Topic

namespace TopicPartition {

/**
 * @brief RdKafka::TopicPartition vector to a v8 Array
 *
 * @see v8ArrayToTopicPartitionVector
 *
 * @note This will destroy the topic partition pointers inside the vector, rendering
 * it unusable
 */
v8::Local<v8::Array> ToV8Array(
  std::vector<RdKafka::TopicPartition*> & topic_partition_list) {  // NOLINT
  v8::Local<v8::Array> array = Nan::New<v8::Array>();
  for (size_t topic_partition_i = 0;
    topic_partition_i < topic_partition_list.size(); topic_partition_i++) {
    RdKafka::TopicPartition* topic_partition =
      topic_partition_list[topic_partition_i];

    // We have the list now let's get the properties from it
    v8::Local<v8::Object> obj = Nan::New<v8::Object>();

    if (topic_partition->offset() != RdKafka::Topic::OFFSET_INVALID) {
      Nan::Set(obj, Nan::New("offset").ToLocalChecked(),
        Nan::New<v8::Number>(topic_partition->offset()));
    }
    Nan::Set(obj, Nan::New("partition").ToLocalChecked(),
      Nan::New<v8::Number>(topic_partition->partition()));
    Nan::Set(obj, Nan::New("topic").ToLocalChecked(),
      Nan::New<v8::String>(topic_partition->topic().c_str()).ToLocalChecked());

    array->Set(topic_partition_i, obj);

    // These are pointers so we need to delete them somewhere.
    // Do it here because we're only going to convert when we're ready
    // to return to v8.
    delete topic_partition;
  }

  // Clear the topic partition list of dangling pointers
  topic_partition_list.clear();

  return array;
}

}  // namespace TopicPartition

namespace Metadata {

/**
 * @brief RdKafka::Metadata to v8::Object
 *
 */
v8::Local<v8::Object> ToV8Object(RdKafka::Metadata* metadata) {
  v8::Local<v8::Object> obj = Nan::New<v8::Object>();

  v8::Local<v8::Array> broker_data = Nan::New<v8::Array>();
  v8::Local<v8::Array> topic_data = Nan::New<v8::Array>();

  const BrokerMetadataList* brokers = metadata->brokers();  // NOLINT

  unsigned int broker_i = 0;

  for (BrokerMetadataList::const_iterator it = brokers->begin();
    it != brokers->end(); ++it, broker_i++) {
    // Start iterating over brokers and set the object up

    const RdKafka::BrokerMetadata* x = *it;

    v8::Local<v8::Object> current_broker = Nan::New<v8::Object>();

    Nan::Set(current_broker, Nan::New("id").ToLocalChecked(),
      Nan::New<v8::Number>(x->id()));
    Nan::Set(current_broker, Nan::New("host").ToLocalChecked(),
      Nan::New<v8::String>(x->host().c_str()).ToLocalChecked());
    Nan::Set(current_broker, Nan::New("port").ToLocalChecked(),
      Nan::New<v8::Number>(x->port()));

    broker_data->Set(broker_i, current_broker);
  }

  unsigned int topic_i = 0;

  const TopicMetadataList* topics = metadata->topics();

  for (TopicMetadataList::const_iterator it = topics->begin();
    it != topics->end(); ++it, topic_i++) {
    // Start iterating over topics

    const RdKafka::TopicMetadata* x = *it;

    v8::Local<v8::Object> current_topic = Nan::New<v8::Object>();

    Nan::Set(current_topic, Nan::New("name").ToLocalChecked(),
      Nan::New<v8::String>(x->topic().c_str()).ToLocalChecked());

    v8::Local<v8::Array> current_topic_partitions = Nan::New<v8::Array>();

    const PartitionMetadataList* current_partition_data = x->partitions();

    unsigned int partition_i = 0;
    PartitionMetadataList::const_iterator itt;

    for (itt = current_partition_data->begin();
      itt != current_partition_data->end(); ++itt, partition_i++) {
      // partition iterate
      const RdKafka::PartitionMetadata* xx = *itt;

      v8::Local<v8::Object> current_partition = Nan::New<v8::Object>();

      Nan::Set(current_partition, Nan::New("id").ToLocalChecked(),
        Nan::New<v8::Number>(xx->id()));
      Nan::Set(current_partition, Nan::New("leader").ToLocalChecked(),
        Nan::New<v8::Number>(xx->leader()));

      const std::vector<int32_t> * replicas  = xx->replicas();
      const std::vector<int32_t> * isrs = xx->isrs();

      std::vector<int32_t>::const_iterator r_it;
      std::vector<int32_t>::const_iterator i_it;

      unsigned int r_i = 0;
      unsigned int i_i = 0;

      v8::Local<v8::Array> current_replicas = Nan::New<v8::Array>();

      for (r_it = replicas->begin(); r_it != replicas->end(); ++r_it, r_i++) {
        current_replicas->Set(r_i, Nan::New<v8::Int32>(*r_it));
      }

      v8::Local<v8::Array> current_isrs = Nan::New<v8::Array>();

      for (i_it = isrs->begin(); i_it != isrs->end(); ++i_it, i_i++) {
        current_isrs->Set(i_i, Nan::New<v8::Int32>(*i_it));
      }

      Nan::Set(current_partition, Nan::New("replicas").ToLocalChecked(),
        current_replicas);
      Nan::Set(current_partition, Nan::New("isrs").ToLocalChecked(),
        current_isrs);

      current_topic_partitions->Set(partition_i, current_partition);
    }  // iterate over partitions

    Nan::Set(current_topic, Nan::New("partitions").ToLocalChecked(),
      current_topic_partitions);

    topic_data->Set(topic_i, current_topic);
  }  // End iterating over topics

  Nan::Set(obj, Nan::New("orig_broker_id").ToLocalChecked(),
    Nan::New<v8::Number>(metadata->orig_broker_id()));

  Nan::Set(obj, Nan::New("orig_broker_name").ToLocalChecked(),
    Nan::New<v8::String>(metadata->orig_broker_name()).ToLocalChecked());

  Nan::Set(obj, Nan::New("topics").ToLocalChecked(), topic_data);
  Nan::Set(obj, Nan::New("brokers").ToLocalChecked(), broker_data);

  return obj;
}

}  // namespace Metadata

namespace Message {

v8::Local<v8::Object> ToV8Object(RdKafka::Message *message) {
  if (message->err() == RdKafka::ERR_NO_ERROR) {
    v8::Local<v8::Object> pack = Nan::New<v8::Object>();

    void* payload = malloc(message->len());
    memcpy(payload, message->payload(), message->len());

    Nan::MaybeLocal<v8::Object> buff = Nan::NewBuffer(
      static_cast<char*>(payload), static_cast<int>(message->len()));

    Nan::Set(pack, Nan::New<v8::String>("value").ToLocalChecked(),
      buff.ToLocalChecked());
    Nan::Set(pack, Nan::New<v8::String>("size").ToLocalChecked(),
      Nan::New<v8::Number>(message->len()));

    if (message->key()) {
      std::string* key = new std::string(*message->key());
      Nan::Set(pack, Nan::New<v8::String>("key").ToLocalChecked(),
        Nan::New<v8::String>(*key).ToLocalChecked());
    } else {
      Nan::Set(pack, Nan::New<v8::String>("key").ToLocalChecked(),
        Nan::Null());
    }

    Nan::Set(pack, Nan::New<v8::String>("topic").ToLocalChecked(),
      Nan::New<v8::String>(message->topic_name()).ToLocalChecked());
    Nan::Set(pack, Nan::New<v8::String>("offset").ToLocalChecked(),
      Nan::New<v8::Number>(message->offset()));
    Nan::Set(pack, Nan::New<v8::String>("partition").ToLocalChecked(),
      Nan::New<v8::Number>(message->partition()));

    return pack;
  } else {
    return RdKafkaError(message->err());
  }
}

}  // namespace Message

}  // namespace Conversion

}  // namespace NodeKafka