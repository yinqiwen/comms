/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "comms.hpp"
#include <algorithm>
#include <math.h>

namespace comms
{
    /*
     *  GEOADD key MERCATOR|WGS84 x y value [x y value....]
     */
    int Comms::GeoAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::GeoPointArray points;
        for (size_t i = 2; i < cmd.GetArguments().size(); i += 3)
        {
            mmkv::GeoPoint point;
            if (!GetDoubleValue(ctx, cmd.GetArguments()[i], point.x)
                    || !GetDoubleValue(ctx, cmd.GetArguments()[i + 1], point.y))
            {
                return 0;
            }
            point.value = cmd.GetArguments()[i + 2];
        }
        int err = m_kv_store->GeoAdd(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], points);
        if (err >= 0)
        {
            if(err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    /*
     *  GEOSEARCH key MERCATOR|WGS84 x y  <GeoOptions>
     *  GEOSEARCH key MEMBER m            <GeoOptions>
     *
     *  <GeoOptions> = [IN N m0 m1 ..] [RADIUS r]
     *                 [ASC|DESC] [WITHCOORDINATES] [WITHDISTANCES]
     *                 [GET pattern [GET pattern ...]]
     *                 [INCLUDE key_pattern value_pattern [INCLUDE key_pattern value_pattern ...]]
     *                 [EXCLUDE key_pattern value_pattern [EXCLUDE key_pattern value_pattern ...]]
     *                 [LIMIT offset count]
     *
     *  For 'GET pattern' in GEOSEARCH:
     *  Pattern would processed the same as 'sort' command (Use same C++ function),
     *  The patterns like '#', "*->field" are valid.
     */

    int Comms::GeoSearch(Context& ctx, RedisCommandFrame& cmd)
    {
        const ArgumentArray& args = cmd.GetArguments();
        const std::string& key = args[0];
        int err = 0;
        mmkv::GeoSearchOptions options;
        for (uint32 i = 1; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "asc"))
            {
                options.asc = true;
            }
            else if (!strcasecmp(args[i].c_str(), "desc"))
            {
                options.asc = false;
            }
            else if (!strcasecmp(args[i].c_str(), "limit") && i < args.size() - 2)
            {
                if (!string_toint32(args[i + 1], options.offset) || !string_toint32(args[i + 2], options.limit))
                {
                    err = mmkv::ERR_INVALID_NUMBER;
                    break;
                }
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "RADIUS") && i < args.size() - 1)
            {
                if (!string_touint32(args[i + 1], options.radius) || options.radius < 1 || options.radius >= 10000000)
                {
                    err = mmkv::ERR_INVALID_NUMBER;
                    break;
                }
                i++;
            }
            else if ((!strcasecmp(args[i].c_str(), "mercator") || !strcasecmp(args[i].c_str(), "wgs84"))
                    && i < args.size() - 2)
            {
                if (!string_todouble(args[i + 1], options.by_x) || !string_todouble(args[i + 2], options.by_y))
                {
                    err = mmkv::ERR_INVALID_COORD_VALUE;
                    break;
                }
                options.coord_type = args[i];
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "member") && i < args.size() - 1)
            {
                options.by_member = args[i + 1];
                i++;
            }

            else if ((!strcasecmp(args[i].c_str(), "GET")) && i < args.size() - 1)
            {
                options.get_patterns.push_back(args[i + 1]);
                i++;
            }
            else if ((!strcasecmp(args[i].c_str(), "include")) && i < args.size() - 2)
            {
                options.includes[args[i + 1]] = args[i + 2];
                i += 2;
            }
            else if ((!strcasecmp(args[i].c_str(), "exclude")) && i < args.size() - 2)
            {
                options.excludes[args[i + 1]] = args[i + 2];
                i += 2;
            }
            else if ((!strcasecmp(args[i].c_str(), GEO_SEARCH_WITH_COORDINATES)))
            {
                options.get_patterns.push_back(GEO_SEARCH_WITH_COORDINATES);
            }
            else if ((!strcasecmp(args[i].c_str(), GEO_SEARCH_WITH_DISTANCES)))
            {
                options.get_patterns.push_back(GEO_SEARCH_WITH_DISTANCES);
            }
            else
            {
                WARN_LOG("Invalid geosearch option:%s", args[i].c_str());
                err = mmkv::ERR_INVALID_ARGS;
                break;
            }
        }
        if (options.by_member.empty() &&  options.by_x == 0 && options.by_y == 0)
        {
            err = mmkv::ERR_INVALID_ARGS;
        }
        if (err < 0)
        {
            FillErrorReply(ctx, err);
            return 0;
        }
        mmkv::StringArrayResult results(ReplyResultStringAlloc, &ctx.reply);
        err = m_kv_store->GeoSearch(ctx.currentDB, key, options, results);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;

    }

}

