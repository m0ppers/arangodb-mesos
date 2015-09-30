////////////////////////////////////////////////////////////////////////////////
/// @brief utilities
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Dr. Frank Celler
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "utils.h"

#include <time.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from a resource
///////////////////////////////////////////////////////////////////////////////

static double diskspaceResource (const mesos::Resource& resource) {
  if (resource.name() == "disk" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from a resource
///////////////////////////////////////////////////////////////////////////////

static double cpusResource (const mesos::Resource& resource) {
  if (resource.name() == "cpus" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from a resource
///////////////////////////////////////////////////////////////////////////////

static double memoryResource (const mesos::Resource& resource) {
  if (resource.name() == "mem" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief computes a FNV hash for strings
////////////////////////////////////////////////////////////////////////////////

uint64_t arangodb::FnvHashString (const vector<string>& texts) {
  uint64_t nMagicPrime = 0x00000100000001b3ULL;
  uint64_t nHashVal = 0xcbf29ce484222325ULL;

  for (auto text : texts) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(text.c_str());
    const uint8_t* e = p + text.size();

    for (; p < e;  ++p) {
      nHashVal ^= *p;
      nHashVal *= nMagicPrime;
    }
  }

  return nHashVal;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief splits a string
////////////////////////////////////////////////////////////////////////////////

vector<string> arangodb::split (const string& value, char separator) {
  vector<string> result;
  string::size_type p = 0;
  string::size_type q;

  while ((q = value.find(separator, p)) != string::npos) {
    result.emplace_back(value, p, q - p);
    p = q + 1;
  }

  result.emplace_back(value, p);
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief joins a vector of string
////////////////////////////////////////////////////////////////////////////////

string arangodb::join (const vector<string>& value, string separator) {
  string result = "";
  string sep = "";

  for (const auto& v : value) {
    result += sep + v;
    sep = separator;
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief jsonify a protobuf message
////////////////////////////////////////////////////////////////////////////////

string arangodb::toJson (::google::protobuf::Message const& msg) {
  string result;
  pbjson::pb2json(&msg, result);
  return result;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::diskspace (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += diskspaceResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::cpus (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += cpusResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::memory (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += memoryResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief converts system time
///////////////////////////////////////////////////////////////////////////////

string arangodb::toStringSystemTime (const chrono::system_clock::time_point& tp) {
  time_t tt = chrono::system_clock::to_time_t(tp);

  char buf[1024];
  strftime(buf, sizeof(buf) - 1, "%F %T", localtime(&tt));

  return buf;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief not-a-port filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::notIsPorts (const mesos::Resource& resource) {
  return resource.name() != "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief a-port filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isPorts (const mesos::Resource& resource) {
  return resource.name() == "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isDisk (const mesos::Resource& resource) {
  return resource.name() == "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-not-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::notIsDisk (const mesos::Resource& resource) {
  return resource.name() != "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-default-role filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isDefaultRole (mesos::Resource const& resource) {
  return ! resource.has_role() || resource.role() == "*";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts number of avaiable ports from an offer, if the given
/// role string is empty, port ranges with any role are counted, otherwise
/// we only count port ranges with that role.
///////////////////////////////////////////////////////////////////////////////

size_t arangodb::numberPorts (mesos::Offer const& offer,
                              std::string const& role) {
  size_t value = 0;

  for (int i = 0; i < offer.resources_size(); ++i) {
    auto const& resource = offer.resources(i);

    if (resource.name() == "ports" &&
        resource.type() == mesos::Value::RANGES) {
      if (role.empty() ||
          (resource.has_role() && resource.role() == role)) {
           // note that role is optional but has a default, therefore
           // has_role() should always be true. Should, for whatever reason,
           // no role be set in the offer, we do not count this range!
        const auto& ranges = resource.ranges();
        
        for (int j = 0; j < ranges.range_size(); ++j) {
          const auto& range = ranges.range(j);

          value += range.end() - range.begin() + 1;
        }
      }
    }
  }

  return value;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the disk resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsDisk (const mesos::Resources& resources) {
  return resources.filter(isDisk);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the non-disk resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterNotIsDisk (const mesos::Resources& resources) {
  return resources.filter(notIsDisk);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the persistent-volume resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsPersistentVolume (const mesos::Resources& resources) {
  return resources.filter(mesos::Resources::isPersistentVolume);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the non-port resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterNotIsPorts (const mesos::Resources& resources) {
  return resources.filter(notIsPorts);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief intersect two sets of resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::intersectResources (mesos::Resources const& a,
                                               mesos::Resources const& b) {
  return a-(a-b);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the default role resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsDefaultRole (const mesos::Resources& resources) {
  return resources.filter(isDefaultRole);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a GET request using libcurl
////////////////////////////////////////////////////////////////////////////////

static size_t WriteMemoryCallback(void* contents, size_t size, size_t nmemb, 
                                  void *userp) {
  size_t realsize = size * nmemb;
  std::string* mem = static_cast<std::string*>(userp);
 
  mem->append((char*) contents, realsize);

  return realsize;
}

int arangodb::doHTTPGet (std::string url, std::string& resultBody) {
  CURL *curl;
  CURLcode res;
 
  curl = curl_easy_init();

  resultBody.clear();

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url << ", curl error: " << res;
    }
    curl_easy_cleanup(curl);
    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a POST request using libcurl, a return value of 0 means
/// OK, the input body is in body, in the end, the body of the result is
/// in resultBody. If libcurl did not initialise properly, -1 is returned.
/// Otherwise, a positive libcurl error code (see man 3 libcurl-errors)
/// is returned.
////////////////////////////////////////////////////////////////////////////////

int arangodb::doHTTPPost (std::string url, std::string const& body,
                                           std::string& resultBody) {
  CURL *curl;
  CURLcode res;
 
  curl = curl_easy_init();

  if (curl) {
    resultBody.clear();

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url << ", curl error: " << res;
    }
    curl_easy_cleanup(curl);
    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
