using MoreLinq;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace cobeNET
{

    public class CobeError : Exception
    {
        public CobeError()
        {

        }

        public CobeError(string message) : base(message)
        {
        }

    }

    // The main interface for Cobe.
    public class Brain
    {

        public Graph graph;

        public Scoring.ScorerGroup scorer;

        public Tokenizer tokenizer;

        public CobeStemmer stemmer;

        public Random random;

        public bool _learning;

        // use an empty string to denote the start/end of a chain
        public static readonly string END_TOKEN = String.Empty;

        // use a magic token id for (single) whitespace, so space is never
        // in the tokens table
        public static readonly long SPACE_TOKEN_ID = -1;

        public int? order;

        public long? _end_token_id;

        public List<string> _end_context;

        public long _end_context_id;



        public Brain(string filename)
        {
            if (!File.Exists(filename))
            {
                Log.Information("File does not exist. Assuming defaults.");

                // File gets created by SQLite
                Brain.init(filename);
            }
            //using (var trace_us("Brain.connect_us"))
            //{
            var connectionStringBuilder = new SQLiteConnectionStringBuilder();

            //Use DB in project directory.  If it does not exist, create it:
            connectionStringBuilder.DataSource = filename;

            this.graph = new Graph(new SQLiteConnection(connectionStringBuilder.ConnectionString));
            //}
            var version = graph.get_info_text("version");
            if (version != "2")
            {
                throw new CobeError(String.Format("cannot read a version {0} brain", version));
            }

            this.order = Convert.ToInt32(graph.get_info_text("order"));
            this.scorer = new Scoring.ScorerGroup();
            this.scorer.add_scorer(1.0, new Scoring.CobeScorer());
            this.random = new Random();
            this._end_context = new List<string>();
            var tokenizer_name = graph.get_info_text("tokenizer");

            if (tokenizer_name == "MegaHAL")
            {
                this.tokenizer = new MegaHALTokenizer();
            }
            else
            {
                this.tokenizer = new CobeTokenizer();
            }

            this.stemmer = null;
            var stemmer_name = graph.get_info_text("stemmer");
            if (stemmer_name != null)
            {
                try
                {
                    this.stemmer = new CobeStemmer(stemmer_name);
                    Log.Debug(String.Format("Initialized a stemmer: {0}", stemmer_name));
                }
                catch(Exception ex)
                {
                    Log.Error(ex, $"Error creating stemmer: {ex.Message}");
                }
            }
            this._end_token_id = graph.get_token_by_text(END_TOKEN, create: true);

            for(long i = 0; i < this.order; i++)
            {
                _end_context.Add(Convert.ToString(_end_token_id));
            }

            this._end_context_id = graph.get_node_by_tokens(this._end_context);
            this._learning = false;
        }

        // Begin a series of batch learn operations. Data will not be
        //         committed to the database until stop_batch_learning is
        //         called. Learn text using the normal learn(text) method.
        public virtual void start_batch_learning()
        {
            this._learning = true;

            SqliteHelper.ExecuteWrite(this.graph._conn, "PRAGMA journal_mode=memory");

            this.graph.drop_reply_indexes();
        }

        // Finish a series of batch learn operations.
        public virtual void stop_batch_learning()
        {
            this._learning = false;

            SqliteHelper.ExecuteWrite(this.graph._conn, "PRAGMA journal_mode=truncate");

            this.graph.ensure_indexes();
        }

        public virtual void del_stemmer()
        {
            this.stemmer = null;
            this.graph.delete_token_stems();
            this.graph.set_info_text("stemmer", null);
            //this.graph.commit();
        }

        public virtual void set_stemmer(string language)
        {
            this.stemmer = new CobeStemmer(language);
            this.graph.delete_token_stems();
            this.graph.update_token_stems(this.stemmer);
            this.graph.set_info_text("stemmer", language);
            //this.graph.commit();
        }

        // Learn a string of text. If the input is not already
        //         Unicode, it will be decoded as utf-8.
        public virtual void learn(string text)
        {
            if (!StringHelper.IsUnicode(text))
            {
                // Assume that non-Unicode text is encoded as utf-8, which
                // should be somewhat safe in the modern world.

                byte[] bytes = Encoding.Default.GetBytes(text);
                text = Encoding.UTF8.GetString(bytes);
            }

            var tokens = this.tokenizer.split(text).ToList();
            //trace("Brain.learn_input_token_count", tokens.Count);
            this._learn_tokens(tokens);
            
        }

        // This is an iterator that returns the nodes of our graph:
        // "This is a test" -> "None This" "This is" "is a" "a test" "test None"
        // 
        // Each is annotated with a boolean that tracks whether whitespace was
        // found between the two tokens.
        public virtual IEnumerable<Tuple<List<string>, bool>> _to_edges(List<string> tokens) 
        {
            // prepend self.order Nones
            //var chain = this._end_context + tokens + this._end_context;
            List<string> chain = new List<string>();
            chain.AddRange(this._end_context.Select(x => Convert.ToString(x)));
            chain.AddRange(tokens);
            chain.AddRange(this._end_context.Select(x => Convert.ToString(x)));

            var has_space = false;
            var context = new List<string>();
            for (int i = 0; i < chain.Count(); i++)
            {
                context.Add(chain[i]);
                if (context.Count == this.order)
                {
                    if (chain[i] == Convert.ToString(SPACE_TOKEN_ID))
                    {
                        context.RemoveAt(context.Count - 1);
                        has_space = true;
                        continue;
                    }

                    var tuple = Tuple.Create(context.ToList(), has_space);

                    context.RemoveAt(0);
                    has_space = false;

                    yield return tuple;
                }
            }
        }

        // This is an iterator that returns each edge of our graph
        // with its two nodes
        public virtual IEnumerable<Tuple<List<string>,bool, List<string>>> _to_graph(IEnumerable<Tuple<List<string>, bool>> contexts)
        {
            Tuple<List<string>, bool> prev = null;
            foreach (var context in contexts)
            {
                if (prev == null)
                {
                    prev = context;
                    continue;
                }
                yield return Tuple.Create(prev.Item1, context.Item2, context.Item1);
                prev = context;
            }
        }

        public virtual void _learn_tokens(List<string> tokens)
        {
            var token_count = (from token in tokens
                               where !string.IsNullOrWhiteSpace(token)
                               select token).ToList().Count;
            if (token_count < 3)
            {
                return;
            }
            // create each of the non-whitespace tokens
            var token_ids = new List<long>();
            foreach (var text in tokens)
            {
                if (string.IsNullOrWhiteSpace(text))
                {
                    token_ids.Add(SPACE_TOKEN_ID);
                    continue;
                }
                var token_id = this.graph.get_token_by_text(text, create: true, stemmer: this.stemmer);
                if(token_id != null)
                    token_ids.Add((long)token_id);
            }
            var edges = this._to_edges(token_ids.Select(x => Convert.ToString(x)).ToList()).ToList();
            long? prev_id = null;
            foreach (var _tup_1 in this._to_graph(edges))
            {
                var prev = _tup_1.Item1;
                var has_space = _tup_1.Item2;
                var next = _tup_1.Item3;
                if (prev_id == null)
                {
                    prev_id = this.graph.get_node_by_tokens(prev);
                }
                var next_id = this.graph.get_node_by_tokens(next);
                this.graph.add_edge(prev_id, next_id, has_space);
                prev_id = next_id;
            }
            if (!this._learning)
            {
                //this.graph.commit();
            }
        }

        // Reply to a string of text. If the input is not already
        //         Unicode, it will be decoded as utf-8.
        public virtual string reply(string text, long loop_ms = 500, long? max_len = null)
        {
            string msg;
            double score;

            if (!StringHelper.IsUnicode(text))
            {
                // Assume that non-Unicode text is encoded as utf-8, which
                // should be somewhat safe in the modern world.

                byte[] bytes = Encoding.Default.GetBytes(text);
                text = Encoding.UTF8.GetString(bytes);
            }


            var tokens = this.tokenizer.split(text).ToList();
            var input_ids = (tokens ?? new List<string>()).Select(x => { return (long)this.graph.get_token_by_text(x); }).ToList();

            // filter out unknown words and non-words from the potential pivots
            var pivot_set = this._filter_pivots(input_ids);
            // Conflate the known ids with the stems of their words
            if (this.stemmer != null)
            {
                this._conflate_stems(ref pivot_set, tokens);
            }
            // If we didn't recognize any word tokens in the input, pick
            // something random from the database and babble.
            if (pivot_set.Count == 0)
            {
                pivot_set = this._babble();
            }
            var score_cache = new Dictionary<object, object>
            {
            };
            var best_score = -1.0;
            Reply best_reply = null;
            // Loop for approximately loop_ms milliseconds. This can either
            // take more (if the first reply takes a long time to generate)
            // or less (if the _generate_replies search ends early) time,
            // but it should stay roughly accurate.
            var start = DateTime.UtcNow;
            var end = start.AddMilliseconds(loop_ms);
            var count = 0;
            var all_replies = new List<object>();
            var _start = DateTime.UtcNow;
            foreach (var _tup_1 in this._generate_replies(pivot_set))
            {
                List<long> edges = _tup_1.Item1;
                var pivot_node = _tup_1.Item2;
                var reply = new Reply(this.graph, tokens, input_ids, pivot_node, edges);

                if (max_len != null)
                {
                    if (this._too_long((long)max_len, reply))
                    {
                        continue;
                    }
                }
                var key = reply.edge_ids;
                if (!score_cache.ContainsKey(key))
                {
                    //using (var trace_us("Brain.evaluate_reply_us"))
                    //{
                        score = this.scorer.score(reply);
                        score_cache[key] = score;
                    //}
                }
                else
                {
                    // skip scoring, we've already seen this reply
                    score = -1;
                }
                if (score > best_score)
                {
                    best_reply = reply;
                    best_score = score;
                }
                // dump all replies to the console if debugging is enabled
                //if (log.isEnabledFor(logging.DEBUG))
                //{
                //    all_replies.append(Tuple.Create(score, reply));
                //}
                count += 1;
                if (DateTime.UtcNow > end)
                {
                    break;
                }
            }
            if (best_reply == null)
            {
                // we couldn't find any pivot words in _babble(), so we're
                // working with an essentially empty brain. Use the classic
                // MegaHAL reply:
                return "I don't know enough to answer you yet!";
            }
            var _time = DateTime.UtcNow - _start;
            this.scorer.end(best_reply);
            //if (log.isEnabledFor(logging.DEBUG))
            //{
            //    var replies = (from _tup_2 in all_replies.Chop((score, reply) => Tuple.Create(score, reply))
            //                   let score = _tup_2.Item1
            //                   let reply = _tup_2.Item2
            //                   select Tuple.Create(score, reply.to_text())).ToList();
            //    replies.sort();
            //    foreach (var _tup_3 in replies)
            //    {
            //        score = _tup_3.Item1;
            //        text = _tup_3.Item2;
            //        Log.Debug("%f %s", score, text);
            //    }
            //}
            //trace("Brain.reply_input_token_count", tokens.Count);
            //trace("Brain.known_word_token_count", pivot_set.Count);
            //trace("Brain.reply_us", _time);
            //trace("Brain.reply_count", count, _time);
            //trace("Brain.best_reply_score", Convert.Tolong32(best_score * 1000));
            //trace("Brain.best_reply_length", best_reply.edge_ids.Count);
            Log.Debug(String.Format("made {0} replies ({1} unique) in {2} seconds", count, score_cache.Count, _time.TotalSeconds));
            if (text.Count() > 60)
            {
                msg = text.Take(60) + "...";
            }
            else
            {
                msg = text;
            }
            Log.Information("[{0}] {1} {2}", msg, count, best_score);
            // look up the words for these tokens
            //using (var trace_us("Brain.reply_words_lookup_us"))
            //{
                text = best_reply.to_text();
            //}
            return text;
        }

        public virtual bool _too_long(long max_len, Reply reply)
        {
            var text = reply.to_text();
            if (text.Count() > max_len)
            {
                Log.Debug("over max_len [{0}]: {1}", text.Count(), text);
                return true;
            }

            return false;
        }

        public virtual void _conflate_stems(ref HashSet<long> pivot_set, List<string> tokens)
        {
            foreach (var token in tokens)
            {
                var stem_ids = this.graph.get_token_stem_id(this.stemmer.stem(token));
                if (stem_ids.Count() < 1) 
                {
                    continue;
                }

                // add the tuple of stems to the pivot set, and then
                // remove the individual token_ids
                pivot_set.UnionWith(stem_ids);
                pivot_set.ExceptWith(stem_ids);
            }
        }

        public virtual HashSet<long> _babble()
        {
            var token_ids = new List<long>();
            for (long i = 0; i < 5; i++)
            {
                // Generate a few random tokens that can be used as pivots
                var token_id = this.graph.get_random_token();
                if (token_id != null)
                {
                    token_ids.Add((long)token_id);
                }
            }
            return new HashSet<long>(token_ids);
        }

        public virtual HashSet<long> _filter_pivots(List<long> pivots)
        {
            // remove pivots that might not give good results
            var tokens = new HashSet<long>(pivots);//(from _f in pivots
                                              //where _f
                                              //select _f).ToList());
            var filtered = this.graph.get_word_tokens(tokens);
            if (filtered == null || filtered.Count() < 1)
            {
                filtered = this.graph.get_tokens(tokens) ?? new List<long>();
            }
            return new HashSet<long>(filtered);
        }

        public virtual long _pick_pivot(List<long> pivot_ids)
        {
            var pivot = pivot_ids[random.Next(pivot_ids.Count())];
            //if (object.ReferenceEquals(type(pivot), tuple))
            //{
            //    // the input word was stemmed to several things
            //    pivot = random.choice(pivot);
            //}
            return pivot;
        }

        public virtual IEnumerable<Tuple<List<long>,int>> _generate_replies(HashSet<long> pivot_ids)
        {
            if (pivot_ids.Count() < 1)
            {
                yield break;
            }
            var end = this._end_context_id;
            var graph = this.graph;
            //var search = graph.search_random_walk; 

            // Cache all the trailing and beginning sentences we find from
            // each random node we search. Since the node is a full n-tuple
            // context, we can combine any pair of next_cache[node] and
            // prev_cache[node] and get a new reply.
            var next_cache = new Dictionary<int, List<long>>();
            var prev_cache = new Dictionary<int, List<long>>();

            while (pivot_ids.Count() > 0)
            {
                // generate a reply containing one of token_ids
                var pivot_id = this._pick_pivot(pivot_ids.ToList());
                var node = graph.get_random_node_with_token(pivot_id);
                var parts = MoreEnumerable.ZipLongest(
                    graph.search_random_walk((long)node, end, true), 
                    graph.search_random_walk((long)node, end, false), (a, b) => new Tuple<List<long>, List<long>>(a,b));

                foreach (var _tup_1 in parts)
                {
                    List<long> next = _tup_1.Item1;
                    List<long> prev = _tup_1.Item2;

                    if (next != null)
                    {
                        if(next_cache.ContainsKey((int)node))
                            next_cache[(int)node].AddRange(next);
                        else
                        {
                            next_cache.Add((int)node, next);
                        }

                        if (prev_cache.ContainsKey((int)node))
                        {
                            foreach (var p in prev_cache[(int)node].ToList())
                            {
                                List<long> nextList = new List<long>();
                                nextList.Add(p);
                                nextList.AddRange(next);
                                yield return Tuple.Create(nextList, (int)node);
                            }
                        }
                    }

                    if (prev.Count() > 0)
                    {
                        prev.Reverse();

                        if (prev_cache.ContainsKey((int)node))
                            prev_cache[(int)node].AddRange(prev);
                        else
                        {
                            prev_cache.Add((int)node, prev);
                        }

                        if (next_cache.ContainsKey((int)node))
                        {
                            foreach (var n in next_cache[(int)node].ToList())
                            {
                                var prevList = next;
                                prevList.Add(n);
                                yield return Tuple.Create(prevList, (int)node);
                            }
                        }
                    }
                }
            }
        }

        // Initialize a brain. This brain's file must not already exist.
        // 
        // Keyword arguments:
        // order -- Order of the forward/reverse Markov chains (INTEGER)
        // tokenizer -- One of Cobe, MegaHAL (default Cobe). See documentation
        //              for cobe.tokenizers for details. (string)
        public static void init(string filename, int order = 3, string tokenizer = null)
        {
            Log.Information(String.Format("Initializing a cobe brain: {0}", filename));
            if (tokenizer == null)
            {
                tokenizer = "Cobe";
            }
            if (!new List<string> { "Cobe", "MegaHAL" }.Contains(tokenizer))
            {
                Log.Information("Unknown tokenizer: {0}. Using CobeTokenizer", tokenizer);
                tokenizer = "Cobe";
            }
            var connectionStringBuilder = new SQLiteConnectionStringBuilder();

            //Use DB in project directory.  If it does not exist, create it:
            connectionStringBuilder.DataSource = filename;

            var graph = new Graph(new SQLiteConnection(connectionStringBuilder.ConnectionString));
            //using (var trace_us("Brain.init_time_us"))
            //{
            graph.init(order, tokenizer);
           // }
        }
    }

    // Provide useful support for scoring functions
    public class Reply
    {
        public Graph graph;
        public object tokens;
        public List<long> token_ids;
        public long pivot_node;
        public List<long> edge_ids;
        public string text;



        public Reply(
            Graph graph,
            object tokens,
            List<long> token_ids,
            long pivot_node,
            List<long> edge_ids)
        {
            this.graph = graph;
            this.tokens = tokens;
            this.token_ids = token_ids;
            this.pivot_node = pivot_node;
            this.edge_ids = edge_ids;
            this.text = null;
        }

        public virtual string to_text()
        {
            if (this.text == null)
            {
                var parts = new List<string>();
                foreach (var _tup_1 in this.edge_ids.Select(x => { return this.graph.get_text_by_edge(x); }))
                {
                    var word = _tup_1.Item1;
                    var has_space = _tup_1.Item2;
                    parts.Add(word);
                    if (has_space)
                    {
                        parts.Add(" ");
                    }
                }
                this.text = String.Join("", parts);
            }
            return this.text;
        }
    }

    // A special-purpose graph class, stored in a sqlite3 database
    public class Graph
    {
        public SQLiteConnection _conn;

        public object row_factory;

        public int order;

        public string _all_tokens;

        public string _all_tokens_args;

        public string _all_tokens_q;

        public string _last_token;

        public Graph(SQLiteConnection conn, bool run_migrations = true)
        {
            this._conn = conn;
            //conn.row_factory = sqlite3.Row;
            if (this.is_initted())
            {
                if (run_migrations)
                {
                    this._run_migrations();
                }
                this.order = Convert.ToInt32(this.get_info_text("order"));
                this._all_tokens = string.Join(",", (from i in Enumerable.Range(0, this.order) select String.Format("token{0}_id", i)).ToList());
                this._all_tokens_args = string.Join(" AND ", (from i in Enumerable.Range(0, this.order) select String.Format("token{0}_id = ?", i)).ToList());
                this._all_tokens_q = string.Join(",", (from i in Enumerable.Range(0, this.order) select "?").ToList());
                this._last_token = String.Format("token{0}_id", this.order - 1);

                // Disable the SQLite cache. Its pages tend to get swapped
                // out, even if the database file is in buffer cache.
                //var c = this.cursor();
                //c.execute("PRAGMA cache_size=0");
                SqliteHelper.ExecuteWrite(this._conn, "PRAGMA cache_size=0");

                //c.execute("PRAGMA page_size=4096");
                SqliteHelper.ExecuteWrite(this._conn, "PRAGMA page_size=4096");

                // Each of these speed-for-reliability tradeoffs is useful for
                // bulk learning.
                //c.execute("PRAGMA journal_mode=truncate");
                SqliteHelper.ExecuteWrite(this._conn, "PRAGMA journal_mode=truncate");

                //c.execute("PRAGMA temp_store=memory");
                SqliteHelper.ExecuteWrite(this._conn, "PRAGMA temp_store=memory");

                //c.execute("PRAGMA synchronous=OFF");
                SqliteHelper.ExecuteWrite(this._conn, "PRAGMA synchronous=OFF");
            }
        }

        //public virtual object cursor()
        //{
        //    return this._conn.cursor();
        //}

        //public virtual object commit()
        //{
        //    //using (var trace_us("Brain.db_commit_us"))
        //    //{
        //        this._conn.commit();
        //    //}
        //}

        public virtual void close()
        {
            this._conn.Close();
        }

        public virtual bool is_initted()
        {
            try
            {
                this.get_info_text("order");
                return true;
            }
            catch(Exception)
            {
                return false;
            }
        }

        public virtual void set_info_text(string attribute, string text)
        {
            //var c = this.cursor();
            //string q;
            if (text == null)
            {
                SqliteHelper.ExecuteWrite(this._conn, "DELETE FROM info WHERE attribute = @attribute", 
                    new Dictionary<string, object>() { 
                        { "@attribute", attribute } 
                    } );
            }
            else
            {
                var rowcount = SqliteHelper.ExecuteWrite(this._conn, "UPDATE info SET text = @text WHERE attribute = @attribute",
                    new Dictionary<string, object>() {
                        { "@text", text },
                        { "@attribute", attribute }
                    });

              
                if (rowcount == 0)
                {
                    SqliteHelper.ExecuteWrite(this._conn, "INSERT INTO info (attribute, text) VALUES (@attribute, @text)",
                    new Dictionary<string, object>() {
                         { "@attribute", attribute },
                        { "@text", text },
                    });

                }
            }
        }

        public virtual string get_info_text(object attribute, string @default = null)
        {
            //var c = this.cursor();
            //if (text_factory != null)
            //{
            //    var old_text_factory = this._conn.text_factory;
            //    this._conn.text_factory = text_factory;
            //}
            var dt = SqliteHelper.Execute(this._conn, "SELECT text FROM info WHERE attribute = @attribute",
                new Dictionary<string, object>()
                {
                    { "@attribute", attribute },
                });

            if (dt == null || dt.Rows.Count == 0)
            {
                return @default;
            }

            var row = dt.Rows[0]; // fetchone
            return (string)row[0];

        }

        public virtual string get_seq_expr(List<long> seq)
        {
            // Format the sequence seq as (item1, item2, item2) as appropriate
            // for an IN () clause in SQL
            if (seq.Count() == 1)
            {
                // Grab the first item from seq. Use an iterator so this works
                // with sets as well as lists.
                return String.Format("({0})", seq.FirstOrDefault());
            }

            return string.Format($"({string.Join(",", seq)})");
        }

        public virtual long? get_token_by_text(string text, bool create = false, CobeStemmer stemmer = null)
        {
            //var c = this.cursor();
            //var q = "SELECT id FROM tokens WHERE text = ?";
            //var row = c.execute(q, Tuple.Create(text)).fetchone();

            var dt = SqliteHelper.Execute(this._conn, "SELECT id FROM tokens WHERE text = @text",
                new Dictionary<string, object>()
                {
                    { "@text", text },
                });

            if (dt != null && dt.Rows.Count > 0)
            {
                var row = dt.Rows[0];
                return (long)row["id"];
            }
            else if (create)
            {
                bool is_word = Regex.IsMatch(text, @"\w");
                var affected = SqliteHelper.ExecuteWriteEffected(this._conn, "INSERT INTO tokens (text, is_word) VALUES (@text, @is_word)", new Dictionary<string, object>()
                {
                    { "@text", text },
                    { "@is_word", is_word },

                });

                var token_id = affected;
                if (stemmer != null)
                {
                    var stem = stemmer.stem(text);
                    if (!string.IsNullOrWhiteSpace(stem))
                    {
                        this.insert_stem(token_id, stem);
                    }
                }
                return token_id;
            }

            return null;
        }

        public virtual void insert_stem(long token_id, string stem)
        {
            SqliteHelper.ExecuteWrite(this._conn, "INSERT INTO token_stems (token_id, stem) VALUES (@tokenid, @stem)", new Dictionary<string, object>()
            {
                { "@tokenid", token_id },
                { "@stem", stem },

            });
        }

        public virtual List<long> get_token_stem_id(string stem)
        {
            var dt = SqliteHelper.Execute(this._conn, "SELECT token_id FROM token_stems WHERE token_stems.stem = @stem", new Dictionary<string, object>()
            {
                { "@stem", stem },
            });

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var rows = dt.Rows;

            return rows.Cast<DataRow>().Select(row => (long)row[0]).ToList(); // map(@operator.itemgetter(0), rows).ToList();

        }

        public virtual List<long> get_word_tokens(IEnumerable<long> token_ids)
        {
            //var q = String.Format("SELECT id FROM tokens WHERE id IN %s AND is_word = 1", this.get_seq_expr(token_ids.ToList()));
            //var rows = this._conn.execute(q);

            if (token_ids.Count() == 0)
            {
                return null;
            }

            var dt = SqliteHelper.Execute(this._conn, string.Format("SELECT id FROM tokens WHERE id IN {0} AND is_word = 1", this.get_seq_expr(token_ids.ToList())));

            //    , this.get_seq_expr(token_ids.ToList())), new Dictionary<string, object>()
            //{
            //    { "@tokenids", this.get_seq_expr(token_ids.ToList()) },
            //}); ;

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var rows = dt.Rows;
          
            return rows.Cast<DataRow>().Select(row => (long)row[0]).ToList();
                //return map(@operator.itemgetter(0), rows).ToList();
           
        }

        public virtual List<long> get_tokens(IEnumerable<long> token_ids)
        {
            // var q = String.Format("SELECT id FROM tokens WHERE id IN %s", this.get_seq_expr(token_ids));

            if (token_ids.Count() == 0)
            {
                return null;
            }

            var dt = SqliteHelper.Execute(this._conn, "SELECT id FROM tokens WHERE id IN @tokenids", new Dictionary<string, object>()
            {
                { "@tokenids", this.get_seq_expr(token_ids.ToList()) },
            });

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var rows = dt.Rows;
            
            return rows.Cast<DataRow>().Select(row => (long)row[0]).ToList();
            //return map(@operator.itemgetter(0), rows).ToList();
         



            //var rows = this._conn.execute(q);
            //if (rows)
            //{
            //    return map(@operator.itemgetter(0), rows).ToList();
            //}
        }

        public virtual long get_node_by_tokens(List<string> tokens)
        {
            //var c = this.cursor();

            var args = string.Join(" AND ", (from i in Enumerable.Range(0, this.order) select String.Format("token{0}_id = {1}", i, tokens[i])));


            var dt = SqliteHelper.Execute(this._conn, string.Format("SELECT id FROM nodes WHERE {0}", args));

            if (dt != null && dt.Rows.Count > 0)
            {
                var row = dt.Rows[0];
                return (long)row[0];
            }

            var q = string.Format("INSERT INTO nodes (count, {0}) VALUES (0,{1})", this._all_tokens, string.Join(",", tokens.Take(this.order)));

            var affected = SqliteHelper.ExecuteWriteEffected(this._conn, q);/*, new Dictionary<string, object>()*/
            //{
            //    { "@all_tokens", this._all_tokens },
            //    { "@all_tokens_q", string.Join(",", tokens).Take(this.order) /*this._all_tokens_q*/ },

            //});

           // if not found, create the node
            return affected;
        }

        public virtual Tuple<string, bool> get_text_by_edge(long edge_id)
        {
            var dt = SqliteHelper.Execute(this._conn, string.Format("SELECT tokens.text, edges.has_space FROM nodes, edges, tokens WHERE edges.id = {0} AND edges.prev_node = nodes.id AND nodes.{1} = tokens.id", edge_id, this._last_token)); //, new Dictionary<string, object>()
            //{
            //    { "@edgeid", edge_id },
            //    { "@lasttoken", this._last_token  }
            //});

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];

            return Tuple.Create((string)row[0], Convert.ToBoolean(row[1]));
        }

        public virtual long? get_random_token()
        {
            // token 1 is the end_token_id, so we want to generate a random token
            // id from 2..max(id) inclusive.
            var dt = SqliteHelper.Execute(this._conn, "SELECT (abs(random()) % (MAX(id)-1)) + 2 FROM tokens");

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];

            if(row[0] != System.DBNull.Value)
                return (long)row[0];

            return null;

        }

        public virtual int? get_random_node_with_token(long token_id)
        {
            //var c = this.cursor();
            var dt = SqliteHelper.Execute(this._conn, "SELECT id FROM nodes WHERE token0_id = @token_id LIMIT 1 OFFSET abs(random())%(SELECT count(*) FROM nodes WHERE token0_id = @token_id)", new Dictionary<string, object>()
            {
                { "@token_id", token_id },
            });

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];

            return Convert.ToInt32(row[0]);
        }

        public virtual double? get_edge_logprob(long edge_id)
        {
            // Each edge goes from an n-gram node (word1, word2, word3) to
            // another (word2, word3, word4). Calculate the probability:
            // P(word4|word1,word2,word3) = count(edge_id) / count(prev_node_id)
            var dt = SqliteHelper.Execute(this._conn, "SELECT edges.count, nodes.count FROM edges, nodes WHERE edges.id = @edge_id AND edges.prev_node = nodes.id", new Dictionary<string, object>()
            {
                { "@edge_id", edge_id },
            });


            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];
            var edge_count = (long)row[0];
            var node_count = (long)row[1];
            return Math.Log(edge_count, 2) - Math.Log(node_count, 2);
        }

        public virtual bool? has_space(long edge_id)
        {
            //var c = this.cursor();
            var dt = SqliteHelper.Execute(this._conn, "SELECT has_space FROM edges WHERE id = @edgeid", new Dictionary<string, object>()
            {
                { "@edgeid", edge_id },
            });

            if (dt == null || dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];

            return Convert.ToBoolean(row[0]);
        }

        public virtual void add_edge(long? prev_node, long next_node, bool has_space)
        {

            var rowcount = SqliteHelper.ExecuteWrite(this._conn, "UPDATE edges SET count = count + 1 WHERE prev_node = @prevnode AND next_node = @nextnode AND has_space = @hasspace", new Dictionary<string, object>()
            {
                { "@prevnode", prev_node },
                { "@nextnode", next_node },
                { "@hasspace", has_space },
            });

         
            if (rowcount == 0)
            {
                SqliteHelper.ExecuteWrite(this._conn, "INSERT INTO edges (prev_node, next_node, has_space, count) VALUES (@prevnode, @nextnode, @hasspace, 1)", new Dictionary<string, object>()
                {
                    { "@prevnode", prev_node },
                    { "@nextnode", next_node },
                    { "@hasspace", has_space },
                });
                // The count on the next_node in the nodes table must be
                // incremented here, to register that the node has been seen an
                // additional time. This is now handled by database triggers.
            }
        }

        //public virtual object search_bfs(long start_id, long end_id, bool direction)
        //{
        //    string q;
        //    if (direction)
        //    {
        //        q = "SELECT id, next_node FROM edges WHERE prev_node = @cur";
        //    }
        //    else
        //    {
        //        q = "SELECT id, prev_node FROM edges WHERE next_node = @cur";
        //    }
            
        //    var left = new List<Tuple<long,List<long>>> { Tuple.Create(start_id, new List<long>()) };
        //    while (left.Count() > 0)
        //    {
        //        var _tup_1 = left.First();
        //        left.RemoveAt(0); // leftpop

        //        var cur = _tup_1.Item1;
        //        var path = _tup_1.Item2;

        //        var rows = SqliteHelper.Execute(this._conn, q, new Dictionary<string, object>()
        //        {
        //            { "@cur", cur },
        //        }).Rows;


        //        foreach (DataRow row in rows)
        //        {
        //            var rowid = (long)row[0];
        //            var next = (long)row[1];
        //            var newpath = path + Tuple.Create(rowid);
        //            if (next == end_id)
        //            {
        //                yield return newpath;
        //            }
        //            else
        //            {
        //                left.AddRange(Tuple.Create(next, newpath));
        //            }
        //        }
        //    }
        //}

        // Walk once randomly from start_id to end_id.
        public virtual IEnumerable<List<long>> search_random_walk(long start_id, long end_id, bool direction)
        {
            string q;
            if (direction)
            {
                q = "SELECT id, next_node FROM edges WHERE prev_node = @last LIMIT 1 OFFSET abs(random())%(SELECT count(*) from edges WHERE prev_node = @last)";
            }
            else
            {
                q = "SELECT id, prev_node FROM edges WHERE next_node = @last LIMIT 1 OFFSET abs(random())%(SELECT count(*) from edges WHERE next_node = @last)";
            }
           

            var left = new List<Tuple<long,List<long>>>() { Tuple.Create(start_id, new List<long>()), };
            while (left.Count() > 0)
            {
                var _tup_1 = left.FirstOrDefault();
                left.RemoveAt(0);

                var cur = _tup_1.Item1;
                var path = _tup_1.Item2;

                var rows = SqliteHelper.Execute(this._conn, q, new Dictionary<string, object>()
                {
                    { "@last", cur },
                }).Rows;


                // Note: the LIMIT 1 above means this list only contains
                // one row. Using a list here so this matches the bfs()
                // code, so the two functions can be more easily combined
                // later.
                foreach (DataRow row in rows)
                {
                    long rowid = (long)row[0];
                    long next = (long)row[1];
                    var newpath = path;
                    newpath.Add(rowid);

                    if (next == end_id)
                    {
                        yield return newpath;
                    }
                    else
                    {
                        left.Add(Tuple.Create(next, newpath));
                    }
                }
            }
        }

        public virtual void init(int order, string tokenizer, bool run_migrations = true)
        {
            Log.Debug("Creating table: info");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TABLE info (
    attribute TEXT NOT NULL PRIMARY KEY,
    text TEXT NOT NULL)");

            Log.Debug("Creating table: tokens");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TABLE tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT UNIQUE NOT NULL,
    is_word INTEGER NOT NULL)");

            var tokens = new List<string>();
            foreach (var i in Enumerable.Range(0, order))
            {
                tokens.Add(String.Format("token{0}_id INTEGER REFERENCES token(id)", i));
            }

            Log.Debug("Creating table: token_stems");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TABLE token_stems (
    token_id INTEGER,
    stem TEXT NOT NULL)");

            Log.Debug("Creating table: nodes");
            SqliteHelper.ExecuteWrite(this._conn, String.Format(@"
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    count INTEGER NOT NULL,
    {0})", string.Join(",\n    ", tokens)));

            Log.Debug("Creating table: edges");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TABLE edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prev_node INTEGER NOT NULL REFERENCES nodes(id),
    next_node INTEGER NOT NULL REFERENCES nodes(id),
    count INTEGER NOT NULL,
    has_space INTEGER NOT NULL)");

            if (run_migrations)
            {
                this._run_migrations();
            }
            // save the order of this brain
            this.set_info_text("order", Convert.ToString(order));
            this.order = order;
            // save the tokenizer
            this.set_info_text("tokenizer", tokenizer);
            // save the brain/schema version
            this.set_info_text("version", "2");
            //this.commit();
            this.ensure_indexes();
            //this.close();
        }

        public virtual void drop_reply_indexes()
        {
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS edges_all_next");
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS edges_all_prev");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE INDEX IF NOT EXISTS learn_index ON edges
    (prev_node, next_node)");
        }

        public virtual void ensure_indexes()
        {
            //var c = this.cursor();
            // remove the temporary learning index if it exists
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS learn_index");
            var token_ids = string.Join(",", (from i in Enumerable.Range(0, this.order)
                                      select String.Format("token{0}_id", i)).ToList());
            SqliteHelper.ExecuteWrite(this._conn, String.Format(@"
CREATE UNIQUE INDEX IF NOT EXISTS nodes_token_ids on nodes
    ({0})", token_ids));
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE UNIQUE INDEX IF NOT EXISTS edges_all_next ON edges
    (next_node, prev_node, has_space, count)");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE UNIQUE INDEX IF NOT EXISTS edges_all_prev ON edges
    (prev_node, next_node, has_space, count)");
        }

        public virtual void delete_token_stems()
        {
            //var c = this.cursor();
            // drop the two stem indexes
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS token_stems_stem");
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS token_stems_id");
            // delete all the existing stems from the table
            SqliteHelper.ExecuteWrite(this._conn, "DELETE FROM token_stems");
            //this.commit();
        }

        public virtual void update_token_stems(CobeStemmer stemmer)
        {
            // stemmer is a CobeStemmer
            //using (var trace_ms("Db.update_token_stems_ms"))
            //{
            //c = this.cursor();
            //insert_c = this.cursor();
            //insert_q = "INSERT INTO token_stems (token_id, stem) VALUES (@token_id, @stem)";
            var q = SqliteHelper.Execute(this._conn, @"SELECT id, text FROM tokens").Rows;
                foreach (DataRow row in q)
                {
                    var stem = stemmer.stem((string)row[1]);
                    if (!string.IsNullOrWhiteSpace(stem))
                    {
                        var insert_q = "INSERT INTO token_stems (token_id, stem) VALUES (@tokenid, @stem)";
                        SqliteHelper.ExecuteWrite(this._conn, insert_q, new Dictionary<string, object>()
                        {
                            { "@tokenid", row[0] },
                            { "@stem", stem },
                        });
                    }
                }
            //this.commit();
            //}
            //using (var trace_ms("Db.index_token_stems_ms"))
            //{
            SqliteHelper.ExecuteWrite(this._conn, @"CREATE INDEX token_stems_id on token_stems (token_id)");
            SqliteHelper.ExecuteWrite(this._conn, @"CREATE INDEX token_stems_stem on token_stems (stem)");
            //}
        }

        public virtual void _run_migrations()
        {
            //using (var trace_us("Db.run_migrations_us"))
            //{
                this._maybe_drop_tokens_text_index();
                this._maybe_create_node_count_triggers();
            //}
        }

        public virtual void _maybe_drop_tokens_text_index()
        {
            // tokens_text was an index on tokens.text, deemed redundant since
            // tokens.text is declared UNIQUE, and sqlite automatically creates
            // indexes for UNIQUE columns
            SqliteHelper.ExecuteWrite(this._conn, "DROP INDEX IF EXISTS tokens_text");
        }

        public virtual void _maybe_create_node_count_triggers()
        {
            // Create triggers on the edges table to update nodes counts.
            // In previous versions, the node counts were updated with a
            // separate query. Moving them INTO triggers improves
            // performance.
            //var c = this.cursor();
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TRIGGER IF NOT EXISTS edges_insert_trigger AFTER INSERT ON edges
    BEGIN UPDATE nodes SET count = count + NEW.count
        WHERE nodes.id = NEW.next_node; END;");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TRIGGER IF NOT EXISTS edges_update_trigger AFTER UPDATE ON edges
    BEGIN UPDATE nodes SET count = count + (NEW.count - OLD.count)
        WHERE nodes.id = NEW.next_node; END;");
            SqliteHelper.ExecuteWrite(this._conn, @"
CREATE TRIGGER IF NOT EXISTS edges_delete_trigger AFTER DELETE ON edges
    BEGIN UPDATE nodes SET count = count - old.count
        WHERE nodes.id = OLD.next_node; END;");
        }
    }
}
