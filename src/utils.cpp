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

using namespace std;
using namespace mesos;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from a resource
///////////////////////////////////////////////////////////////////////////////

static double diskspaceResource (const mesos::Resource& resource) {
  if (resource.name() == "disk" && resource.type() == Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from a resource
///////////////////////////////////////////////////////////////////////////////

static double cpusResource (const mesos::Resource& resource) {
  if (resource.name() == "cpus" && resource.type() == Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from a resource
///////////////////////////////////////////////////////////////////////////////

static double memoryResource (const mesos::Resource& resource) {
  if (resource.name() == "mem" && resource.type() == Value::SCALAR) {
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

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::diskspace (const Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += diskspaceResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::cpus (const Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += cpusResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::memory (const Resources& resources) {
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

bool arangodb::notIsPorts (const Resource& resource) {
  return resource.name() != "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief a-port filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isPorts (const Resource& resource) {
  return resource.name() == "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isDisk (const Resource& resource) {
  return resource.name() == "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-not-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::notIsDisk (const Resource& resource) {
  return resource.name() != "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts number of avaiable ports from an offer
///////////////////////////////////////////////////////////////////////////////

size_t arangodb::numberPorts (const mesos::Offer& offer) {
  size_t value = 0;

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);

    if (resource.name() == "ports" &&
        resource.type() == mesos::Value::RANGES) {
      const auto& ranges = resource.ranges();
      
      for (int j = 0; j < ranges.range_size(); ++j) {
        const auto& range = ranges.range(j);

        value += range.end() - range.begin() + 1;
      }
    }
  }

  return value;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
