// NOTE : requires nodejs 7

var parse = require('csv-parse');
var transform = require('stream-transform');
var fs = require('fs');
var moment = require('moment');

// const files = ['2015-09', '2015-10', '2015-11', '2015-12', '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11'];
const files = ['2015-10', '2015-11', '2015-12', '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11'];
let file_index = 0;
let previous_timestamp = moment(0);
let previous_diff=0;

const doit = (file_name) => {
  console.log('processing ' + file_name)

  let input = fs.createReadStream('./proc/' + file_name + '-owl.csv');
  let output = fs.createWriteStream('./proc/' + file_name + '-owl-diff.csv');

  let parser = parse({
    delimiter: ',',
    columns: (data) => {
      return ['timestamp', 'curr_chan3'];
    }
  });

  const transformer = transform((record, callback) => {
    setImmediate(() => {

      console.log('record', record);

      let current_timestamp = moment(record[0], "YYYY-MM-DD HH:mm:ss");
      let current_diff = current_timestamp.diff(previous_timestamp);
      let start_time = '';
      let end_time = '';

      console.log('current timestamp', current_timestamp);
      console.log('current diff', current_diff);

      // c1 = this timestamp-last time stamp
      //=IF((C1>65) + (C1<0), A2, "")
      // 60000


      let row = (record.curr_chan3 !== '0') ? record[0] + ',' + record[1] + ','  : null;

      if (row) {
        if (current_diff > 60000){
          end_time = current_timestamp;
        }

        if (previous_diff > 60000) {
          start_time = current_timestamp;
        }

        row += start_time + ',' + end_time + '\n';
      }

      previous_timestamp = current_timestamp;
      previous_diff = current_diff;

      callback(null, row);
    });
  }, {parallel: 10} );

  output.on('close', () => {
    console.log('output close');
    if (file_index < files.length) {
      doit(files[file_index]);
    }
  });

  input
    .pipe(parser)
    .pipe(transformer)
    // .pipe(output);

  file_index++;
}

doit(files[file_index]);
