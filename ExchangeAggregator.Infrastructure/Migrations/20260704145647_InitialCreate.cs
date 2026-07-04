using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace ExchangeAggregator.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ticks",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Exchange = table.Column<string>(type: "varchar(50)", nullable: false),
                    Ticker = table.Column<string>(type: "varchar(20)", nullable: false),
                    Price = table.Column<decimal>(type: "numeric(18,8)", nullable: false),
                    Volume = table.Column<decimal>(type: "numeric(18,8)", nullable: false),
                    Timestamp = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    ReceivedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ticks", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ticks_Exchange_Ticker_Timestamp",
                table: "ticks",
                columns: new[] { "Exchange", "Ticker", "Timestamp" });

            migrationBuilder.CreateIndex(
                name: "IX_ticks_ReceivedAt",
                table: "ticks",
                column: "ReceivedAt");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ticks");
        }
    }
}
