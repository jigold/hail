#!/usr/bin/env node

'use strict';

process.on('uncaughtException', function (err) {
  console.log('Caught exception: ' + err);
  process.exit(1);
});

const tutorialHtmlTemplate = __dirname + "/" + process.argv[2];
const pandocOutputDir = __dirname + "/" + process.argv[3];
const compiledHTMLOutputFile = __dirname + "/" + process.argv[4];

const jsdom = require('jsdom');
const fs = require('fs');

const mjAPI = require("mathjax-node/lib/mj-page.js");

mjAPI.start();

jsdom.env(tutorialHtmlTemplate, function (err, window) {
    window.addEventListener("error", function (event) {
      console.error("script error!!", event.error);
      process.exit(1);
    });

    const $ = require('jquery')(window);

    function loadReq(selector, file) {
        return new Promise(function (resolve, reject) {
            const s = $(selector);
            if (s.length == 0) {
                reject("No elements found for selector " + selector);
            } else {
                $(selector).load(file, function (response, status, xhr) {
                    if (status == "error") {
                        console.log("error when loading file: " + file);
                        reject(status)
                    } else {
                        resolve(response);
                    }
                });
            }
        });
    }

    function error(message) {
        console.log(message);
        process.exit(1);
    }

    var loadTutorialPromises =  ["Tutorial"].map(name => loadReq("#" + name, pandocOutputDir + name + ".html"));

    Promise.all(loadTutorialPromises)
            .then(function() {
                var document = jsdom.jsdom($('html').html());

                mjAPI.typeset({
                html: document.body.innerHTML,
                renderer: "SVG",
                inputs: ["TeX"],
                xmlns: "mml"
                }, function(result) {
                document.body.innerHTML = result.html;
                var HTML = "<!DOCTYPE html>\n" + document.documentElement.outerHTML.replace(/^(\n|\s)*/, "");
                fs.writeFile(compiledHTMLOutputFile, HTML);
                });
            }, error);
});