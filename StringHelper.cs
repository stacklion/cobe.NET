using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace cobeNET
{
    public static class StringHelper
    {
        public static bool IsUnicode(string input)
        {
            return !string.IsNullOrEmpty(input);
            //const int MaxAnsiCode = 255;

            //return input.Any(c => c > MaxAnsiCode);
        }

    }
}
