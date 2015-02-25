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

using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

namespace arangodb {

////////////////////////////////////////////////////////////////////////////////
/// @brief computes a FNV hash for strings
////////////////////////////////////////////////////////////////////////////////

  uint64_t FnvHashString (const vector<string>& texts) {
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

  vector<string> split (const string& value, char separator) {
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

  string join (const vector<string>& value, string separator) {
    string result = "";
    string sep = "";

    for (const auto& v : value) {
      result += sep + v;
      sep = separator;
    }

    return result;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
