# Admin Dashboard Next Steps

Firstly, I'm wondering how we can best represent limit order status in the admin dashboard because limit orders are different from market orders. Market orders have, basically, we're waiting for funding, and once we get funding, we instantly execute. We immediately begin executing and completing the whole process. But for limit orders we know that there may be an initial step once the funding occurs to move those funds Into Hyperliquid in such a way that we're actually able to create the true limit order. I want a status that represents: this is an order sitting on Hyperliquid's limit order book. I think our current statuses on the limit order side are too market-order shaped, so we may need to rethink them. Give me your best effort attempt at creating the admin dashboard row and any other status fields on the columns of the limit order page, because the market order page just looks great. I want to change that, so this is only going to apply to the limit order page. Give me your best effort at fixing this.


Secondly, I'm noticing that it takes a really long time to load when I click market orders. I have to toggle between market orders and limit orders; it takes a genuinely long time to load them, and I'm wondering why that's the case. I'm seeing that the orders API endpoint literally took two seconds in this case, which is interesting.Please figure out why this is happening and fix it if there's an obvious reason. If you can't find the reason, let me know, but dig deep.

Thirdly, I want a way to search for an order ID and view it as a row, either as a market row or a limit row.

Fourthly, I want to be able to sort and not include certain types of orders. For example, if I'm looking at market orders, I only want there to be an option, maybe a dropdown, that provides the cleanest solution from a UX perspective.

Expose a way to show only in‑progress orders—orders that have a funding transaction but are not yet completed. Include a button for that. Add a button to see all failed orders.

Failed basically means refunded or something that did not go right. Actually, there should be a distinction: refunded orders and orders that have failed and didn’t get refunded. That is the manual refund flow.

Provide a default “fire hose” view. In all cases, the list should be chronologically sorted, with the latest items at the top.

Finally, do this after the other ones are completely done. I want a chart view of volume over time, ideally an actual volume chart that I can arbitrarily change the window I'm looking at. I can also change the bucket size. For example, I can pick a daily bucket, an hourly bucket, or a minute‑by‑minute bucket. Test, test, test. This is ultimately going to involve a secondary database that is a derivative of our replica. It doesn't have to be a PostgreSQL database; it can be whatever database is ideal for this type of data.

That's up to you. It must be open source and trivial to deploy to Railway, but you can choose any database you believe is good for this specific use case. The input is the entire corpus of all orders, and I would like to see simply the net volume among all completed orders—market orders and limit orders that are fully completed.

Limit orders don't count if they are still sitting on the order book; they only count when fully completed and thus relevant to add to our volume number at that moment. When an order becomes completed, it can contribute to the volume number for that bucket.

It should be highly optimized, with no computation done by our backend. We should pre‑compute all these buckets, store them historically in an appropriate database, provide a way to serve that information, and finally render it at the top of our admin dashboard.


Remember, be idiomatic. do not worry about backwards compat at all
