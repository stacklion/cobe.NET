using System;
using System.Collections.Generic;
using System.Text;

namespace cobeNET
{
    public static class TimeHelper
    {
        /// <summary>
        /// time.time() will return the current time as a float which represents seconds since 1/1/1970
        /// </summary>
        /// <returns></returns>
        public static float Time()
        {
            TimeSpan t = (DateTime.UtcNow - new DateTime(1970, 1, 1));
            return (float)t.TotalSeconds;
        }

    }
}
