﻿namespace premium_sb_samples
{
    public static class Constants
    {
        public static class SampleQueueNames
        {
            public const string q_send_receive = "q_send_receive";
            public const string q_send_schedule = "q_send_schedule";
            public const string q_dead_letter_peeked_msgs = "q_dead_letter_peeked_msgs";
            public const string q_large_msg_send_receive = "q_large_msg_send_receive";
            public const string q_send_defer = "q_send_defer";
            public const string q_send_receive_autottlmsg_dead_letter = "q_send_receive_autottlmsg_dead_letter";
            public const string q_send_autottlmsg_dead_letter_set_defer = "q_send_autottlmsg_dead_letter_set_defer";
            public const string q_send_autottlmsg_dead_letter_set_defer_complete = "q_send_autottlmsg_dead_letter_set_defer_complete";
        }
    }
}