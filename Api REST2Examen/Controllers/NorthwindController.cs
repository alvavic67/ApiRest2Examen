using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;

namespace Api_REST2Examen.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    [RoutePrefix("v1/Analysis/Northwind")]
    public class NorthwindController : ApiController
    {
        [HttpGet]
        [Route("GetItemByDimension/{dim}/{order}")]
        public HttpResponseMessage GetItemByDimension(string dim, string order = "DESC")
        {
            string WITH = @"
                WITH
                SET [OrderDimension] AS
                NONEMPTY(
                    ORDER(
                        {0}.CHILDREN,
                        {0}.CURRENTMEMBER.MEMBER_NAME, " + order +
                    @")
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            WITH = string.Format(WITH, dim);
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;
            
            List<string> dimension = new List<string>();

            dynamic result = new
            {
                datosDimension = dimension,
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpGet]
        [Route("GetMesDimension")]
        public HttpResponseMessage GetMesDimension()
        {
            string WITH = @"
                WITH
                SET [OrderDimension] AS
                NONEMPTY(
                    ORDER(
                        [Dim Tiempo].[Mes Espaniol].CHILDREN,
                        [Dim Tiempo].[Mes Espaniol].CURRENTMEMBER.MEMBER_NAME
                    )
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> dimension = new List<string>();

            dynamic result = new
            {
                datosMeses = dimension,
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpGet]
        [Route("GetAnioDimension")]
        public HttpResponseMessage GetAnioDimension()
        {
            string WITH = @"
                WITH
                SET [OrderDimension] AS
                NONEMPTY(
                    ORDER(
                        [Dim Tiempo].[Anio].CHILDREN,
                        [Dim Tiempo].[Anio].CURRENTMEMBER.MEMBER_NAME, DESC
                    )
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> dimension = new List<string>();

            dynamic result = new
            {
                datosAnios = dimension,
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpPost]
        [Route("GetDataPieDimension/{dim}/{order}")]
        public HttpResponseMessage GetDataPieDimension(string dim, [FromBody] dynamic values)
        {
            string WITH = @"
            WITH
                SET [Order dimension] AS
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                    [Measures].[Fact Ventas Ventas], DESC
                    )
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                    ([OrderDimension], STRTOSET(@Anios), STRTOSET(@Meses))
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> dimension = new List<string>();
            List<string> anios = new List<string>();
            List<string> meses = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = dimension,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            string valoresDimension = string.Empty;
            foreach (var item in values)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension = valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dim);
            valoresDimension = @"{" + valoresDimension + "}";

            string valoresAnios = string.Empty;
            foreach (var item in values.years)
            {
                valoresAnios += "[Dim Tiempo].[Anio].[" + item + "],";
            }
            valoresAnios = valoresAnios.TrimEnd(',');
            valoresAnios = @"{" + valoresAnios + "}";

            string valoresMeses = string.Empty;
            foreach (var item in values.months)
            {
                valoresMeses += "[Dim Tiempo].[Mes Espaniol].[" + item + "],";
            }
            valoresMeses = valoresMeses.TrimEnd(',');
            valoresMeses = @"{" + valoresMeses + "}";

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    cmd.Parameters.Add("Anios", valoresAnios);
                    cmd.Parameters.Add("Meses", valoresMeses);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));
                            anios.Add(dr.GetString(1));
                            meses.Add(dr.GetString(2));
                            ventas.Add(Math.Round(dr.GetDecimal(3)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                anios = dr.GetString(1),
                                meses = dr.GetString(2),
                                valor = Math.Round(dr.GetDecimal(3))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }
    }
}
