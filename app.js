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

var express = require('express');
var request = require('request');
var sprintf = require('sprintf').sprintf;
var async = require("async");
var urllib = require("url");

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// The port that this express app will listen on
var port = process.env.PORT || 7464;

// Your client ID and secret from http://dev.singly.com/apps
var s3 = new require('./s3').backend({key:process.env.S3_KEY, secret:process.env.S3_SECRET, bucket:process.env.S3_BUCKET});

// Create an HTTP server
var app = express();

// Setup for the express web framework
app.configure(function() {
  app.use(express.logger());
  app.use(express.bodyParser());
  app.use(app.router);
});

// We want exceptions and stracktraces in development
app.configure('development', function() {
  app.use(express.errorHandler({dumpExceptions: true, showStack: true}));
});

// Render out views/index.ejs, passing in the session
app.get('/', function(req, res) {
  res.end("nothing to see here, move along");
});

app.post('/drain/:service/:user', function(req, res){
  if(!Array.isArray(req.body)) {
    console.log("bad data", req.body);
    return;
  }
  var buckets = {};
  var adding = {};
  var syncerr;
  // dump into per-day buckets
  req.body.forEach(function(entry){
    if(entry && (entry.type == "error" || entry.type == "stop")) syncerr = entry;
    if(!entry || entry.type != "data" || !entry.data || !entry.data.created_at) return;
    entry.user = req.params.user;
    renormalize(entry);
    if(!buckets[entry.day]) buckets[entry.day] = [];
    buckets[entry.day].push(entry);
    adding[entry.id] = true;
  });

  async.forEach(Object.keys(buckets), function(day, cbDay){
    var dest = req.params.service+"/"+req.params.user+"/"+day+".json";
    s3.get(dest, function(err, buf){
      if(err) return cbDay(err);
      var existing = [];
      if(buf) try{ existing = JSON.parse(buf) } catch(E){ console.log("couldn't parse", dest, buf.toString()); };
      // skip any being added again
      existing.forEach(function(entry){ if(!adding[entry.id] && dayok(entry)) buckets[day].push(entry); });
      s3.put(dest, new Buffer(JSON.stringify(buckets[day])), cbDay);
    });
  }, function(err){
    if(err) {
      console.log("failed",err);
      return res.send(500)      
    }

    var index = {};
    Object.keys(buckets).forEach(function(day){
      index[day] = buckets[day].length;
    })
    res.send(200);
    var dest = req.params.service+"/"+req.params.user+"/index.json";
    console.log("Saved", dest, JSON.stringify(index), syncerr);

    // update index
    s3.get(dest, function(err, buf){
      var existing = {};
      if(buf) try{ existing = JSON.parse(buf) } catch(E){ console.log("couldn't parse", dest, buf.toString()); };
      if(!existing.days) existing.days = {};
      existing.synced = Date.now();
      if(syncerr) {
        existing.error = syncerr;
      }else{
        delete existing.error;
      }
      Object.keys(index).forEach(function(day){ existing.days[day] = index[day]; });
      s3.put(dest, new Buffer(JSON.stringify(existing)), function(err){
        if(err) console.log("failed to save",dest,err);
      });
    })
  });
});

app.get('/index/:service/:user', function(req, res){
  var dest = req.params.service+"/"+req.params.user+"/index.json";
  s3.get(dest, function(err, buf){
    var existing = {};
    if(buf) try{ existing = JSON.parse(buf) } catch(E){ console.log("couldn't parse", dest, buf.toString()); };
    res.json(existing);
  });
});

function renormalize(entry)
{
  var old = entry.data;
  if(old.type == "photo")
  {
    entry.image_url = old.url;
    entry.image_thumbnail = old.thumbnail_url;
  }
  entry.at = old.created_at;
  entry.data = entry.raw;
  delete entry.raw;
  entry.id = entry.entry_id.toString();
  var idr = {};
  idr.protocol = old.type;
  idr.auth = entry.user;
  idr.host = entry.service;
  idr.pathname = "/"+entry.category;
  idr.hash = entry.id;
  entry.idr = urllib.format(idr);
  var dayte = new Date(entry.at);
  function pad(n){return n<10 ? '0'+n : n};
  entry.day = [dayte.getUTCFullYear(), pad(dayte.getUTCMonth()+1), pad(dayte.getUTCDate())].join("-");
}

// check old entries to make sure they're ok
function dayok(entry)
{
  if(!entry.day) return true; // only validate ones we created
  var dayte = new Date(entry.at);
  function pad(n){return n<10 ? '0'+n : n};
  var day = [dayte.getUTCFullYear(), pad(dayte.getUTCMonth()+1), pad(dayte.getUTCDate())].join("-");
  if(entry.day != day) return false;
  return true;
}


app.listen(port);

console.log(sprintf('Listening on port %s', port));
