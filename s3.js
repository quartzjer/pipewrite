/*
* Copyright (C) 2013 Singly, Inc. All Rights Reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*    * Redistributions of source code must retain the above copyright
*      notice, this list of conditions and the following disclaimer.
*    * Redistributions in binary form must reproduce the above copyright
*      notice, this list of conditions and the following disclaimer in the
*      documentation and/or other materials provided with the distribution.
*    * Neither the name of the Locker Project nor the
*      names of its contributors may be used to endorse or promote products
*      derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL THE LOCKER PROJECT BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

var knox = require("knox");
var zlib = require('compress-buffer');

// ***********************************************
//
// S3-based storage with compression
//
// ***********************************************
exports.backend = function (args) {

  // TODO, add one auto-retry on 500
  args.acl = args.acl || "bucket-owner-full-control";

  this.client = knox.createClient(args);

  this.get = function (key, cbDone) {
    var startTime = Date.now();
    var headers = {"Content-Type": "x-json/gz"};
    var req = this.client.get(key, headers);

    req.on("response", function (res) {

      // Dump debugging info about response
      //
      // TODO: Should logger.debug be efficient and let us avoid this additional
      // if statement?
      if (args.debug) {
        args.debug("S3 GET status:" + res.statusCode + " key:" + key);
        args.debug(res.headers);
      }

      if (res.statusCode === 200) {
        var buffer = new Buffer(0);

        res.on("data", function (chunk) {
          buffer = Buffer.concat([buffer, chunk]);
        });

        res.on("end", function () {
          cbDone(null, zlib.uncompress(buffer));
        });
//      } else if(res.statusCode === 404){
//        cbDone();
      } else {
        var msg = "";
        res.on("data", function (data) { msg += data.toString(); });
        res.on("end", function () {
          cbDone(new Error("S3 GET error: " + res.statusCode + " key:" + key + " " + msg.toString()), null);
        });
      }
    });

    // Execute the request
    return req.end();
  };

  this.put = function (key, buffer, cbDone) {
    buffer = zlib.compress(buffer);
    var req = this.client.put(key, {
      "Content-Length": buffer.length,
      "Content-Type": "x-json/gz",
      "x-amz-acl": args.acl
    });

    req.on("response", function (res) {
      // Dump debugging info about response
      if (args.debug) {
        args.debug("S3 PUT status:" + res.statusCode + " key:" + key);
        args.debug(res.headers);
      }

      if (res.statusCode === 200) {
        cbDone(null);
      } else {
        var msg = "";
        res.on("data", function (data) { msg += data.toString(); });
        res.on("end", function () {
          cbDone(new Error("S3 PUT error: " + res.statusCode + " key: " + key + " " + msg.toString()));
        });
      }
    });
    req.end(buffer);
  };

  return this;
};
