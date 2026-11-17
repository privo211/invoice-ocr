query 50142 "Kamterter Posted Assemblies"
{
    QueryType = Normal;

    elements
    {
        dataitem(Assembly; "Posted Assembly Header")
        {
            DataItemTableFilter = Reversed = const(false);

            column(OrderNo; "Order No.") { }
            column(AssemblyNo; "No.") { }

            dataitem(AssemblyLine; "Posted Assembly Line")
            {
                DataItemLink = "Document No." = Assembly."No.";
                SqlJoinType = LeftOuterJoin;

                column(LineType; Type) { }
                column(ResourceNo; "No.") { }
                column(Quantity; Quantity) { }
                column(UnitOfMeasure; "Unit of Measure Code") { }
            }
        }
    }
}
