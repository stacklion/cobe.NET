using Iveonik.Stemmers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace cobeNET
{
    public abstract class Tokenizer
    {

        public abstract List<string> split(string phrase);

        public abstract string join(List<string> words);

    }

    public class MegaHALTokenizer : Tokenizer
    {
        public override List<string> split(string phrase)
        {
            if (!StringHelper.IsUnicode(phrase))
                throw new Exception("Input must be Unicode");

            if (phrase.Length == 0)
                return new List<string>();

            // Do we have a dot?
            if (!".!?".Contains(phrase.Last()))
            {
                phrase += ".";
            }

            var words = Regex.Matches(phrase.ToUpper(), @"([A-Z']+|[0-9]+|[^A-Z'0-9]+)")
                .Cast<Match>()
                .Select(m => m.Value)
                .ToList();

            return words;
        }

        public override string join(List<string> words)
        {
            //Capitalize the first alpha character in the reply and the
            //first alpha character that follows one of[.?!] and a
            //space.

            List<char> chars = string.Join("", words).ToList();
            bool start = true;

            for (int i = 0; i < chars.Count(); i++)
            {
                var letter = chars[i];

                if (char.IsLetter(letter))
                {
                    if (start)
                        chars[i] = char.ToUpper(letter);
                    else
                        chars[i] = char.ToLower(letter);

                    start = false;

                }
                else
                {
                    if (i > 2 && ".?!".Contains(chars.ElementAt(i - 1)) && char.IsWhiteSpace(letter))
                        start = true;
                }
            }

            return string.Join("", chars);
        }
    }

    public class CobeTokenizer : Tokenizer
    {
        Regex regex;

        public CobeTokenizer()
        {
            //# Add hyphen to the list of possible word characters, so hyphenated
            //# words become one token (e.g. hy-phen). But don't remove it from
            //# the list of non-word characters, so if it's found entirely within
            //# punctuation it's a normal non-word (e.g. :-( )

            regex = new Regex(@"(\w+:\S+" + // urls
                @"|[\w'-]+" + // words
                @"|[^\w\s][^\w]*[^\w\s]" + // multiple punctuation
                @"|[^\w\s]" + //  a single punctuation character
                @"|\s+)", RegexOptions.Compiled); // White space
        }

        public override List<string> split(string phrase)
        {
            if (!StringHelper.IsUnicode(phrase))
               throw new Exception("Input must be Unicode");

            // Strip leading and trailing whitespace. This might not be the
            // correct choice long-term, but in the brain it prevents edges
            // from the root node that have has_space set.
            phrase = phrase.Trim();

            if (phrase.Length == 0)
                return new List<string>();

            //List<string> tokens = regex phrase.Split(' ').Where(x => regex.IsMatch(x)).ToList();

            // Search for valid words
            List<string> tokens = regex.Matches(phrase)
                .Cast<Match>()
                .Select(m => m.Value)
                .ToList();

            //List<string> tokenList = new List<string>();

            // collapse runs of whitespace into a single space
            char space = ' ';
            for(int i = 0; i < tokens.Count; i++)
            {
                var token = tokens[i];

                if (char.IsWhiteSpace(token[0]) && token.Length > 1)
                    tokens[i] = space.ToString();
            }


            return tokens;
        }

        public override string join(List<string> words)
        {
            return string.Join("", words);
        }
    }

    public class CobeStemmer
    {
        IStemmer stemmer;

        public CobeStemmer(string name)
        {
            // use the Snowball stemmer bindings
            this.stemmer = new EnglishStemmer();
        }

        public string stem(string token)
        {
            if (!Regex.IsMatch(token, @"\w"))
                return stem_nonword(token);

            // Don't preserve case when stemming, i.e. create lowercase stems.
            // This will allow us to create replies that switch the case of
            // input words, but still generate the reply in context with the
            // generated case.

            var stem = stemmer.Stem(token.ToLower());

            return stem;
        }

        public string stem_nonword(string token)
        {
            // Stem common smile and frown emoticons down to :) and :(
            if(Regex.IsMatch(token, @":-?[ \)]*\)"))
                return ":)";

            if (Regex.IsMatch(token, @":-?[' \(]*\("))
                return ":(";

            return token;
        }
    }
}
