using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace cobeNET
{
    public static class Scoring
    {

        public abstract class Scorer
        {

            public Dictionary<object, object> cache;

            public Scorer()
            {
                this.cache = new Dictionary<object, object>
                {
                };
            }

            public virtual void end(object reply)
            {
                this.cache = new Dictionary<object, object>
                {
                };
            }

            public virtual double normalize(double score)
            {
                // map high-valued scores into 0..1
                if (score < 0)
                {
                    return score;
                }
                return 1.0 - 1.0 / (1.0 + score);
            }

            public abstract double score(Reply reply);
            //{
            //    throw new NotImplementedException();
            //}
        }

        public class ScorerGroup
        {
            public List<Tuple<double, Scorer>> scorers { get; set; }

            double total_weight;

            public ScorerGroup()
            {
                this.scorers = new List<Tuple<double, Scorer>>();
            }

            public virtual void add_scorer(double weight, Scorer scorer)
            {
                // add a scorer with a negative weight if you want to reverse
                // its impact
                this.scorers.Add(Tuple.Create(weight, scorer));
                var total = 0.0;
                foreach (var _tup_1 in this.scorers)
                {
                    weight = _tup_1.Item1;
                    var scorers = _tup_1.Item2;
                    total += Math.Abs(weight);
                }
                this.total_weight = total;
            }

            public virtual void end(Reply reply)
            {
                foreach (var scorer in this.scorers)
                {
                    scorer.Item2.end(reply);
                }
            }

            public virtual double score(Reply reply)
            {
                // normalize to 0..1
                var score = 0.0;
                foreach (var _tup_1 in this.scorers)
                {
                    var weight = _tup_1.Item1;
                    var scorer = _tup_1.Item2;
                    var s = scorer.score(reply);
                    // make sure score is in our accepted range
                    //Debug.Assert(0.0 <= score <= 1.0);
                    if (weight < 0.0)
                    {
                        s = 1.0 - s;
                    }
                    score += Math.Abs(weight) * s;
                }
                return score / this.total_weight;
            }
        }

        // Classic Cobe scorer
        public class CobeScorer
            : Scorer
        {

            public override double score(Reply reply)
            {
                var edge_ids = reply.edge_ids;
                var info = 0.0;
                var logprob_cache = new Dictionary<object, double>();
                //var logprob_cache = this.cache.setdefault("logprob", new Dictionary<object, object>
                //{
                //});
                var space_cache = new Dictionary<object, bool>();
                //var space_cache = this.cache.setdefault("has_space", new Dictionary<object, object>
                //{
                //});
                // var get_edge_logprob = reply.graph.get_edge_logprob;
                //var has_space = reply.graph.has_space;
                // Calculate the information content of the edges in this reply.
                foreach (int edge_id in edge_ids)
                {
                    if (!logprob_cache.ContainsKey(edge_id))
                    {
                        logprob_cache[edge_id] = (double)reply.graph.get_edge_logprob(edge_id);
                    }
                    info -= logprob_cache[edge_id];
                }
                // Approximate the number of cobe 1.2 contexts in this reply, so the
                // scorer will have similar results.
                // First, we have (graph.order - 1) extra edges on either end of the
                // reply, since cobe 2.0 learns from (_END_TOKEN, _END_TOKEN, ...).
                var n_words = edge_ids.Count() - (reply.graph.order - 1) * 2;
                // Add back one word for each space between edges, since cobe 1.2
                // treated those as separate parts of a context.
                foreach (var edge_id in edge_ids)
                {
                    if (!space_cache.ContainsKey(edge_id))
                    {
                        space_cache[edge_id] = (bool)reply.graph.has_space(edge_id);
                    }
                    if (space_cache[edge_id])
                    {
                        n_words += 1;
                    }
                }
                // Double the score, since Cobe 1.x scored both forward and backward
                info *= 2.0;
                // Comparing to Cobe 1.x scoring:
                // At this point we have an extra count for every space token
                // that adjoins punctuation. I'm tweaking the two checks below
                // for replies longer than 16 and 32 tokens (rather than our
                // original 8 and 16) as an adjustment. Scoring is an ongoing
                // project.
                if (n_words > 16)
                {
                    info /= Math.Sqrt(n_words - 1);
                }
                else if (n_words >= 32)
                {
                    info /= n_words;
                }
                return this.normalize(info);
            }
        }

        // Score based on the information of each edge in the graph
        public class InformationScorer
            : Scorer
        {

            public override double score(Reply reply)
            {
                var edge_ids = reply.edge_ids;
                var info = 0.0;
                var logprob_cache = new Dictionary<object, double>();
                //var get_edge_logprob = reply.graph.get_edge_logprob;
                // Calculate the information content of the edges in this reply.
                foreach (var edge_id in edge_ids)
                {
                    if (!logprob_cache.ContainsKey(edge_id))
                    {
                        logprob_cache[edge_id] = (double)reply.graph.get_edge_logprob(edge_id);
                    }
                    info -= logprob_cache[edge_id];
                }
                return this.normalize(info);
            }
        }

        public class LengthScorer
            : Scorer
        {

            public override double score(Reply reply)
            {
                return this.normalize(reply.edge_ids.Count());
            }
        }
    }
}
