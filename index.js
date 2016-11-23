var parse = require('csv-parse');
var transform = require('stream-transform');
var fs = require('fs');

var files = ['2015-05', '2015-06', '2015-07', '2015-08', '2015-09', '2015-10', '2015-11', '2015-12', '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11'];

const parser = parse({
  delimiter: ',',
  columns: true
});

const transformer = transform((record, callback) => {
  setImmediate(() => {
    let row = (record.curr_chan3 !== '0') ? record.timestamp + ',' + record.curr_chan3 + '\n' : null;
    callback(null, row);
  });
}, {parallel: 10} );


const doit = (file_name) => {
  console.log('processing ' + file_name)
  let output = fs.createWriteStream('./proc/' + file_name + '-owl.csv');
  let input = fs.createReadStream('./csv/' + file_name + '-owl.csv');
  input
    .pipe(parser)
    .pipe(transformer)
    .pipe(output)
}

parser.on('finish', () => {

});