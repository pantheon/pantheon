# Pantheon UI

This project is a UI for [Pantheon](https://github.com/pantheon/pantheon).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Getting Started

1. Clone the repo
1. `cd` into the folder
1. Install dependencies: `npm install`
1. Start Pantheon backend at port 4300
1. Start UI `npm run "start:local"`

## Configuration

This app uses a [`config.js`](https://github.com/contiamo/contiamo-ui/blob/master/public/config.js) file that tells it which backends to talk to. This represents a configMap in kubernetes that we use in the cloud to make parts of the app dynamic.

## Building for Production

Create static build in a `dist` folder with `npm run build`.
