import { db } from "@/lib/prisma";
import { inngest } from "./client";
import { GoogleGenerativeAI } from "@google/generative-ai";

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });

export const generateIndustryInsights = inngest.createFunction(
  { name: "Generate Industry Insights" },
  { cron: "0 0 * * 0" }, // Run every Sunday at midnight
  async ({ event, step }) => {
    // First, let's check all industries and their nextUpdate dates
    const allIndustries = await db.$queryRaw`
      SELECT industry, "nextUpdate", "lastUpdated"
      FROM "IndustryInsight"
      ORDER BY "nextUpdate" ASC
    `;
    console.log('All industries in database:', allIndustries);

    const industries = await step.run("Fetch industries", async () => {
      // Use raw SQL to find industries where nextUpdate is in the past OR lastUpdated is more than 7 days ago
      const results = await db.$queryRaw`
        SELECT industry, "nextUpdate", "lastUpdated"
        FROM "IndustryInsight" 
        WHERE "nextUpdate" <= NOW() 
        OR "lastUpdated" <= NOW() - INTERVAL '7 days'
        ORDER BY "nextUpdate" ASC
      `;
      console.log(`Found ${results.length} industries to update:`, results);
      return results;
    });

    if (industries.length === 0) {
      console.log('No industries found that need updating. Current time:', new Date());
      return;
    }

    for (const { industry } of industries) {
      console.log(`Processing industry: ${industry}`);
      
      const prompt = `
          Analyze the current state of the ${industry} industry and provide insights in ONLY the following JSON format without any additional notes or explanations:
          {
            "salaryRanges": [
              { "role": "string", "min": number, "max": number, "median": number, "location": "string" }
            ],
            "growthRate": number,
            "demandLevel": "High" | "Medium" | "Low",
            "topSkills": ["skill1", "skill2"],
            "marketOutlook": "Positive" | "Neutral" | "Negative",
            "keyTrends": ["trend1", "trend2"],
            "recommendedSkills": ["skill1", "skill2"]
          }
          
          IMPORTANT: Return ONLY the JSON. No additional text, notes, or markdown formatting.
          Include at least 5 common roles for salary ranges.
          Growth rate should be a percentage.
          Include at least 5 skills and trends.
        `;

      const res = await step.ai.wrap(
        "gemini",
        async (p) => {
          return await model.generateContent(p);
        },
        prompt
      );

      const text = res.response.candidates[0].content.parts[0].text || "";
      const cleanedText = text.replace(/```(?:json)?\n?/g, "").trim();

      const insights = JSON.parse(cleanedText);
      console.log(`Generated insights for ${industry}:`, insights);

      try {
        await step.run(`Update ${industry} insights`, async () => {
          const nextUpdateDate = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
          console.log(`Updating ${industry} with next update date: ${nextUpdateDate}`);
          
          const updated = await db.industryInsight.update({
            where: { industry },
            data: {
              ...insights,
              lastUpdated: new Date(),
              nextUpdate: nextUpdateDate,
            },
          });
          
          console.log(`Successfully updated ${industry}:`, updated);
          return updated;
        });
      } catch (error) {
        console.error(`Failed to update ${industry}:`, error);
        throw error;
      }
    }
  }
);