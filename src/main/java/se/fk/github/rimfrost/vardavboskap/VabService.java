package se.fk.github.rimfrost.vardavboskap;

import se.fk.rimfrost.VahKundbehovsflodeRequestMessageData;
import se.fk.rimfrost.VahKundbehovsflodeResponseMessageData;
import se.fk.rimfrost.framework.regel.Utfall;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class VabService
{

   public String startProcess(VahKundbehovsflodeRequestMessageData kundbehovsflodeRequest)
   {
      System.out.print("VabService.startProcess\n");
      System.out.printf("triggered by VahKundbehovsflodeRequestMessageData: %s%n", kundbehovsflodeRequest.toString());
      var kundbehovsflodeId = kundbehovsflodeRequest.getKundbehovsflodeId();
      System.out.printf("Started vård av boskap process for kundbehovsflode %s%n", kundbehovsflodeId);
      return kundbehovsflodeId;
   }

   public VahKundbehovsflodeResponseMessageData informAboutDecision(String kundbehovsflodeId, Utfall utfall)
   {
      System.out.printf("vab application for kundbehovsflodeId %s finished with result %s !%n", kundbehovsflodeId, utfall);
      VahKundbehovsflodeResponseMessageData vahKundbehovsflodeResponseMessageData = new VahKundbehovsflodeResponseMessageData();
      vahKundbehovsflodeResponseMessageData.setKundbehovsflodeId(kundbehovsflodeId);
      vahKundbehovsflodeResponseMessageData.setResultat(utfall == Utfall.JA ? "GODKÄND" : "EJ GODKÄND");
      return vahKundbehovsflodeResponseMessageData;
   }

}
