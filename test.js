var fs = require('fs-extra');

require('./index').testrun({
    dbhost: 'localhost',
    dbport: 3306,
    dbname: 'phpbb3',
    dbuser: 'user',
    dbpass: 'password',

    tablePrefix: 'phpbb_'
}, function(err, results) {
    fs.writeFileSync('./tmp.json', JSON.stringify(results, undefined, 2));
});