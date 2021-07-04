
var async = require('async');
var mysql = require('mysql');
var _ = require('lodash');
var extend = require('extend');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-phpbb]';

(function(Exporter) {

    Exporter.setup = function(config, callback) {
        Exporter.log('setup');

        // mysql db only config
        // extract them from the configs passed by the nodebb-plugin-import adapter
        var _config = {
            host: config.dbhost || config.host || 'localhost',
            user: config.dbuser || config.user || 'root',
            password: config.dbpass || config.pass || config.password || '',
            port: config.dbport || config.port || 3306,
            database: config.dbname || config.name || config.database || 'phpbb',
        };

        Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		config.custom = config.custom || {};
		if (typeof config.custom === 'string') {
			try {
				config.custom = JSON.parse(config.custom)
			} catch (e) {}
		}

		config.custom = config.custom || {};
		config.custom.timemachine = config.custom.timemachine || {};
		config.custom = extend(true, {}, {
			timemachine: {
				messages: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				users: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				topics: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				categories: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				posts: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				}
			}
		}, config.custom);

        Exporter.config('custom', config.custom);

        Exporter.connection = mysql.createConnection(_config);
        Exporter.connection.connect();

		setInterval(function() {
			Exporter.connection.query("SELECT 1", function(){});
		}, 60000);

		callback(null, Exporter.config());
    };

    Exporter.query = function(query, callback) {
		if (!Exporter.connection) {
			var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		console.log('\n\n====QUERY====\n\n' + query + '\n');
		Exporter.connection.query(query, function(err, rows) {
			if (rows) {
				console.log('returned: ' + rows.length + ' results');
			}
			callback(err, rows)
		});
	};

    Exporter.getUsers = function(callback) {
        return Exporter.getPaginatedUsers(0, -1, callback);
    };
    Exporter.getPaginatedUsers = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var query = 'SELECT '
            + prefix + 'users.user_id as _uid, '
            + prefix + 'users.username as _username, '
            + prefix + 'users.username_clean as _alternativeUsername, '
            + prefix + 'users.user_email as _email, '
            + prefix + 'users.group_id as _group, ' // ?
            + prefix + 'users.user_birthday as _birthday, '
            + prefix + 'users.user_lastpost_time as _lastposttime, '
            + prefix + 'users.user_rank as _rank, '
            + prefix + 'users.user_sig as _signature, '
            + prefix + 'users.user_avatar as _picture, '
            + prefix + 'user_group.group_id as _groups, ' // ?
            + prefix + 'users.user_regdate as _joindate '

            + ' FROM ' + prefix + 'users '
            + ' LEFT JOIN ' + prefix + 'user_group ON ' + prefix + 'user_group.user_id = ' + prefix + 'users.user_id '
            + ' WHERE 1 = 1 '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    // from unix timestamp (s) to JS timestamp (ms)
                    row._joindate = (row._joindate || 0) * 1000
                    row._lastposttime = ((row._lastposttime || 0) * 1000)

                    row._groups = _.uniq([].concat(row._groups).concat(row._group).filter(g => g))

                    // lower case the email for consistency
                    row._email = (row._email || '').toLowerCase();

                    row._picture = row._picture ? `/uploads/imported_avatars/${row._picture}` : null

                    row._path = `/memberlist.php?mode=viewprofile&u=${row._uid}`

                    map[row._uid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';

		var query = 'SELECT '
			+ prefix + 'groups.group_id as _gid, '
			+ prefix + 'groups.group_name as _name, '
			+ prefix + 'groups.group_desc as _description, '
			+ prefix + 'user_group.user_id as _ownerUId '

			+ ' FROM ' + prefix + 'groups '
            + ' LEFT JOIN ' + prefix + 'user_group ON ' + prefix + 'user_group.group_id = ' + prefix + 'groups.group_id AND ' + prefix + 'user_group.group_leader = 1 '
			+ ' WHERE '	+ prefix + 'groups.group_id >= 0 '

			+ (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
					map[row._gid] = row;
                    row._name = (row._name || '').toLowerCase()
                    row._description = (row._description || '').toLowerCase()
				});

				callback(null, map);
			});
	};

    Exporter.getMessages = function(callback) {
		return Exporter.getPaginatedMessages(0, -1, callback);
	};

	Exporter.getPaginatedMessages = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';

		var query = 'SELECT '
			+ prefix + 'privmsgs.msg_id as _mid, '
			+ prefix + 'privmsgs.author_id as _fromuid, '
			+ prefix + 'privmsgs.to_address as _touid, '
			+ prefix + 'privmsgs.message_text as _content, '
			+ prefix + 'privmsgs.message_time as _timestamp '
			+ 'FROM ' + prefix + 'privmsgs '
			+ (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
                    row._timestamp = ((row._timestamp || 0) * 1000);
                    // don't know why to_address looks like "u_${user_id}"
					row._touid = parseInt(('' + row._touid).replace(/^u_/, ''))
					map[row._mid] = row;
				});

				callback(null, map);
			});
	};


    Exporter.getCategories = function(callback) {
        return Exporter.getPaginatedCategories(0, -1, callback);
    };
    Exporter.getPaginatedCategories = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'forums.forum_id as _cid, '
            + prefix + 'forums.parent_id as _parentCid, '
            + prefix + 'forums.forum_name as _name, '
            + prefix + 'forums.forum_desc as _description '
            + 'FROM ' + prefix + 'forums '
            +  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._name = row._name || 'Untitled Category';
                    row._description = row._description || 'No decsciption available';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    row._path = `/viewforum.php?f=${row._cid}`

                    map[row._cid] = row;
                });

                callback(null, map);
            });
    };


    var getAttachmentsMap = function (callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');

		if (Exporter._attachmentsMap) {
			return callback(null, Exporter._attachmentsMap);
		}

		var query = 'SELECT '
			+ prefix + 'attachments.attach_id as _aid, '
			+ prefix + 'attachments.poster_id as _uid, '
            + prefix + 'attachments.topic_id as _tid, '
            + prefix + 'attachments.post_msg_id as _pid, '
			+ prefix + 'attachments.physical_filename as _url, '
            + prefix + 'attachments.real_filename as _filename, '
            + prefix + 'attachments.download_count as _downloads, '
            + prefix + 'attachments.filetime as _timestamp '

			+ 'FROM ' + prefix + 'attachments '

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
                var map = {};
                rows.forEach(function(row) {
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    row._url = `/uploads/imported_attachments/${row._url || 'N/A'}`
                    map[`${row._tid}_${row._pid}`] = map[`${row._tid}_${row._pid}`] || [];
                    map[`${row._tid}_${row._pid}`].push(row)
                });
				Exporter._attachmentsMap = map;
				callback(null, map);
			});
	};


    Exporter.getTopics = function(callback) {
        return Exporter.getPaginatedTopics(0, -1, callback);
    };
    Exporter.getPaginatedTopics = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var err;
        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query =
            'SELECT '
            + prefix + 'topics.forum_id as _cid, '
            + prefix + 'topics.topic_id as _tid, '
            + prefix + 'topics.topic_poster as _uid, '
            + prefix + 'topics.topic_first_post_id as _mainPid, '
            + prefix + 'topics.topic_views as _viewcount, '
            + prefix + 'topics.topic_title as _title, '
            + prefix + 'topics.topic_time as _timestamp, '
            + prefix + 'topics.topic_delete_time as _deleted, '
            + prefix + 'topics.topic_status as _status, '
            + prefix + 'posts.post_text as _content, '
            + prefix + 'posts.poster_ip as _ip, '
            + prefix + 'posts.post_edit_time as _edited, '
            + prefix + 'posts.post_username as _guest '
            + ' FROM ' + prefix + 'topics '
            + ' LEFT JOIN ' + prefix + 'posts ON ' + prefix + 'posts.post_id = ' + prefix + 'topics.topic_first_post_id '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        getAttachmentsMap(function (err, attachmentsMap) {
            if (err) {
                Exporter.error(err);
                return callback(err);
            }

            Exporter.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }

                    //normalize here
                    var map = {};
                    rows.forEach(function(row) {
                        row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
                        row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                        row._deleted = row._deleted ? 1 : 0
                        row._edited = row._edited ? row._edited * 1000 : null
                        row._path = `/viewtopic.php?f=${row._cid}&t=${row._tid}`
                        row._attachments = attachmentsMap[`${row._tid}_${row._mainPid}`] || []
                        map[row._tid] = row;
                    });

                    callback(null, map);
                });
        })
    };

	var getTopicsMainPids = function(callback) {
		if (Exporter._topicsMainPids) {
			return callback(null, Exporter._topicsMainPids);
		}
		Exporter.getPaginatedTopics(0, -1, function(err, topicsMap) {
			if (err) return callback(err);

			Exporter._topicsMainPids = {};
			Object.keys(topicsMap).forEach(function(_tid) {
				var topic = topicsMap[_tid];
				Exporter._topicsMainPids[topic.topic_first_post_id] = topic._tid;
			});
			callback(null, Exporter._topicsMainPids);
		});
	};
    Exporter.getPosts = function(callback) {
        return Exporter.getPaginatedPosts(0, -1, callback);
    };
    Exporter.getPaginatedPosts = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query =
            'SELECT '
            + prefix + 'posts.forum_id as _cid, '
            + prefix + 'posts.topic_id as _tid, '
            + prefix + 'posts.post_id as _pid, '
            + prefix + 'posts.post_time as _timestamp, '
            + prefix + 'posts.post_subject as _subject, '
            + prefix + 'posts.post_text as _content, '
            + prefix + 'posts.poster_id as _uid, '
            + prefix + 'posts.poster_ip as _ip, '
            + prefix + 'posts.post_edit_time as _edited, '
            + prefix + 'posts.post_delete_time as _deleted, '
            + prefix + 'posts.post_username as _guest '
            + 'FROM ' + prefix + 'posts '
            + 'WHERE ' + prefix + 'posts.topic_id > 0 '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
			function (err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
                getAttachmentsMap(function (err, attachmentsMap) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }
                    getTopicsMainPids(function(err, mpids) {
                        if (err) {
                            Exporter.error(err);
                            return callback(err);
                        }
                        //normalize here
                        var map = {};
                        rows.forEach(function (row) {
                            // make it's not a topic
                            if (! mpids[row._pid]) {
                                row._content = row._content || '';
                                row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                                row._deleted = row._deleted ? 1 : 0
                                row._edited = row._edited ? row._edited * 1000 : null
                                row._path = `/viewtopic.php?p=${row._pid}`
                                row._attachments = attachmentsMap[`${row._tid}_${row._pid}`] || []
                                map[row._pid] = row;
                            }
                        });
                        callback(null, map);
                    });

                })
			});

    };

    Exporter.teardown = function(callback) {
        Exporter.log('teardown');
        Exporter.connection.end();

        Exporter.log('Done');
        callback();
    };

    Exporter.testrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getUsers(next);
            },
            function(next) {
                Exporter.getGroups(next);
            },
            function(next) {
                Exporter.getMessages(next);
            },
            function(next) {
                Exporter.getCategories(next);
            },
            function(next) {
                Exporter.getTopics(next);
            },
            function(next) {
                Exporter.getPosts(next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.paginatedTestrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getPaginatedUsers(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedGroups(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedMessages(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedCategories(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedTopics(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedPosts(1001, 2000, next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.warn = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.warn.apply(console, args);
    };

    Exporter.log = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.log.apply(console, args);
    };

    Exporter.error = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.error.apply(console, args);
    };

    Exporter.config = function(config, val) {
        if (config != null) {
            if (typeof config === 'object') {
                Exporter._config = config;
            } else if (typeof config === 'string') {
                if (val != null) {
                    Exporter._config = Exporter._config || {};
                    Exporter._config[config] = val;
                }
                return Exporter._config[config];
            }
        }
        return Exporter._config;
    };

    // from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
    Exporter.validateUrl = function(url) {
        var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
        return url && url.length < 2083 && url.match(pattern) ? url : '';
    };

    Exporter.truncateStr = function(str, len) {
        if (typeof str != 'string') return str;
        len = _.isNumber(len) && len > 3 ? len : 20;
        return str.length <= len ? str : str.substr(0, len - 3) + '...';
    };

    Exporter.whichIsFalsy = function(arr) {
        for (var i = 0; i < arr.length; i++) {
            if (!arr[i])
                return i;
        }
        return null;
    };

})(module.exports);
